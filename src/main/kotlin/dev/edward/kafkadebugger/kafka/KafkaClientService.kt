package dev.edward.kafkadebugger.kafka

import dev.edward.kafkadebugger.model.*
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import java.time.Duration
import java.util.Properties
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

object KafkaClientService {

    private inline fun <T> withPluginClassLoader(block: () -> T): T {
        val thread = Thread.currentThread()
        val prev = thread.contextClassLoader
        thread.contextClassLoader = KafkaClientService::class.java.classLoader
        return try {
            block()
        } finally {
            thread.contextClassLoader = prev
        }
    }

    fun buildBaseProperties(cluster: ClusterConfig): Properties {
        return Properties().apply {
            put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers.trim())
            put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, "10000")
            put(CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG, "12000")
            put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, cluster.securityProtocol.kafkaValue)

            if (cluster.securityProtocol == SecurityProtocol.SSL ||
                cluster.securityProtocol == SecurityProtocol.SASL_SSL
            ) {
                if (cluster.sslTruststoreLocation.isNotBlank()) {
                    put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, cluster.sslTruststoreLocation.trim())
                }
                if (cluster.sslTruststorePassword.isNotBlank()) {
                    put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, cluster.sslTruststorePassword)
                }
            }

            if (cluster.securityProtocol == SecurityProtocol.SASL_PLAINTEXT ||
                cluster.securityProtocol == SecurityProtocol.SASL_SSL
            ) {
                put(SaslConfigs.SASL_MECHANISM, cluster.saslMechanism.kafkaValue)
                val loginModule = when (cluster.saslMechanism) {
                    SaslMechanism.PLAIN -> "org.apache.kafka.common.security.plain.PlainLoginModule"
                    SaslMechanism.SCRAM_SHA_256, SaslMechanism.SCRAM_SHA_512 ->
                        "org.apache.kafka.common.security.scram.ScramLoginModule"
                }
                val user = escapeJaas(cluster.saslUsername)
                val pass = escapeJaas(cluster.saslPassword)
                put(
                    SaslConfigs.SASL_JAAS_CONFIG,
                    "$loginModule required username=\"$user\" password=\"$pass\";"
                )
            }

            cluster.extraProperties.forEach { (k, v) ->
                if (k.isNotBlank()) put(k.trim(), v)
            }
        }
    }

    private fun escapeJaas(value: String): String =
        value.replace("\\", "\\\\").replace("\"", "\\\"")

    fun testConnection(cluster: ClusterConfig): ConnectionTestResult = withPluginClassLoader {
        val start = System.currentTimeMillis()
        try {
            AdminClient.create(buildBaseProperties(cluster)).use { admin ->
                val result = admin.describeCluster()
                val clusterId = result.clusterId().get()
                val nodes = result.nodes().get()
                ConnectionTestResult(
                    success = true,
                    clusterId = clusterId,
                    brokerCount = nodes.size,
                    latencyMs = System.currentTimeMillis() - start
                )
            }
        } catch (e: Exception) {
            ConnectionTestResult(
                success = false,
                latencyMs = System.currentTimeMillis() - start,
                errorMessage = e.cause?.message ?: e.message ?: e.toString()
            )
        }
    }

    fun listTopics(cluster: ClusterConfig, includeInternal: Boolean = false): List<TopicInfo> =
        withPluginClassLoader {
            AdminClient.create(buildBaseProperties(cluster)).use { admin ->
                val listings = admin.listTopics(ListTopicsOptions().listInternal(includeInternal)).listings().get()
                val names = listings.map { it.name() }.sorted()
                if (names.isEmpty()) return@withPluginClassLoader emptyList()

                val descriptions = admin.describeTopics(names).allTopicNames().get()
                val allTopicPartitions = descriptions.values.flatMap { desc ->
                    desc.partitions().map { p -> TopicPartition(desc.name(), p.partition()) }
                }

                val beginOffsets = if (allTopicPartitions.isNotEmpty()) {
                    admin.listOffsets(allTopicPartitions.associateWith { OffsetSpec.earliest() }).all().get()
                        .mapValues { it.value.offset() }
                } else emptyMap()

                val endOffsets = if (allTopicPartitions.isNotEmpty()) {
                    admin.listOffsets(allTopicPartitions.associateWith { OffsetSpec.latest() }).all().get()
                        .mapValues { it.value.offset() }
                } else emptyMap()

                names.mapNotNull { name ->
                    val desc = descriptions[name] ?: return@mapNotNull null
                    val partitions = desc.partitions().map { p ->
                        val tp = TopicPartition(name, p.partition())
                        PartitionInfo(
                            partition = p.partition(),
                            leaderId = p.leader()?.id() ?: -1,
                            replicaIds = p.replicas().map { it.id() },
                            isrIds = p.isr().map { it.id() },
                            beginningOffset = beginOffsets[tp] ?: 0L,
                            endOffset = endOffsets[tp] ?: 0L
                        )
                    }.sortedBy { it.partition }

                    TopicInfo(
                        name = name,
                        internal = desc.isInternal,
                        partitionCount = partitions.size,
                        replicationFactor = desc.partitions().firstOrNull()?.replicas()?.size ?: 0,
                        partitions = partitions
                    )
                }
            }
        }

    fun getTopicDetail(cluster: ClusterConfig, topic: String): TopicInfo = withPluginClassLoader {
        AdminClient.create(buildBaseProperties(cluster)).use { admin ->
            val desc = admin.describeTopics(listOf(topic)).allTopicNames().get()[topic]
                ?: throw IllegalArgumentException("Topic not found: $topic")

            val tps = desc.partitions().map { TopicPartition(topic, it.partition()) }
            val beginOffsets = admin.listOffsets(tps.associateWith { OffsetSpec.earliest() }).all().get()
                .mapValues { it.value.offset() }
            val endOffsets = admin.listOffsets(tps.associateWith { OffsetSpec.latest() }).all().get()
                .mapValues { it.value.offset() }

            val resource = org.apache.kafka.common.config.ConfigResource(
                org.apache.kafka.common.config.ConfigResource.Type.TOPIC, topic
            )
            val configs = admin.describeConfigs(listOf(resource)).all().get()[resource]
                ?.entries()
                ?.filter { !it.isDefault && it.value() != null }
                ?.associate { it.name() to it.value() }
                ?: emptyMap()

            val partitions = desc.partitions().map { p ->
                val tp = TopicPartition(topic, p.partition())
                PartitionInfo(
                    partition = p.partition(),
                    leaderId = p.leader()?.id() ?: -1,
                    replicaIds = p.replicas().map { it.id() },
                    isrIds = p.isr().map { it.id() },
                    beginningOffset = beginOffsets[tp] ?: 0L,
                    endOffset = endOffsets[tp] ?: 0L
                )
            }.sortedBy { it.partition }

            TopicInfo(
                name = topic,
                internal = desc.isInternal,
                partitionCount = partitions.size,
                replicationFactor = desc.partitions().firstOrNull()?.replicas()?.size ?: 0,
                partitions = partitions,
                configs = configs.toSortedMap()
            )
        }
    }

    fun createTopic(cluster: ClusterConfig, name: String, partitions: Int, replicationFactor: Short): Void? =
        withPluginClassLoader {
            AdminClient.create(buildBaseProperties(cluster)).use { admin ->
                val newTopic = NewTopic(name.trim(), partitions, replicationFactor)
                admin.createTopics(listOf(newTopic)).all().get()
            }
        }

    fun deleteTopic(cluster: ClusterConfig, name: String): Void? = withPluginClassLoader {
        AdminClient.create(buildBaseProperties(cluster)).use { admin ->
            admin.deleteTopics(listOf(name)).all().get()
        }
    }

    fun listConsumerGroups(cluster: ClusterConfig): List<ConsumerGroupInfo> = withPluginClassLoader {
        AdminClient.create(buildBaseProperties(cluster)).use { admin ->
            val groupIds = admin.listConsumerGroups().all().get().map { it.groupId() }.sorted()
            if (groupIds.isEmpty()) return@withPluginClassLoader emptyList()

            val described = admin.describeConsumerGroups(groupIds).all().get()
            groupIds.mapNotNull { gid ->
                val desc = described[gid] ?: return@mapNotNull null
                val offsetsMap = admin.listConsumerGroupOffsets(gid).partitionsToOffsetAndMetadata().get()
                val tps = offsetsMap.keys.toList()
                val endOffsets = if (tps.isNotEmpty()) {
                    admin.listOffsets(tps.associateWith { OffsetSpec.latest() }).all().get()
                        .mapValues { it.value.offset() }
                } else emptyMap()

                val partitionOffsets = offsetsMap.entries
                    .sortedWith(compareBy({ it.key.topic() }, { it.key.partition() }))
                    .map { (tp, meta) ->
                        GroupPartitionOffset(
                            topic = tp.topic(),
                            partition = tp.partition(),
                            committedOffset = meta?.offset() ?: 0L,
                            logEndOffset = endOffsets[tp] ?: 0L
                        )
                    }

                val members = desc.members().map { m ->
                    GroupMemberInfo(
                        memberId = m.consumerId(),
                        clientId = m.clientId(),
                        host = m.host(),
                        assignments = m.assignment().topicPartitions()
                            .sortedWith(compareBy({ it.topic() }, { it.partition() }))
                            .map { "${it.topic()}[${it.partition()}]" }
                    )
                }

                ConsumerGroupInfo(
                    groupId = gid,
                    state = desc.state().toString(),
                    coordinator = desc.coordinator()?.let { "${it.host()}:${it.port()}" } ?: "-",
                    members = members,
                    offsets = partitionOffsets
                )
            }
        }
    }

    fun resetConsumerGroupOffsets(
        cluster: ClusterConfig,
        groupId: String,
        topic: String,
        toLatest: Boolean,
        specificOffset: Long? = null
    ): Void? = withPluginClassLoader {
        AdminClient.create(buildBaseProperties(cluster)).use { admin ->
            val desc = admin.describeTopics(listOf(topic)).allTopicNames().get()[topic]
                ?: throw IllegalArgumentException("Topic not found: $topic")
            val tps = desc.partitions().map { TopicPartition(topic, it.partition()) }

            val newOffsets: Map<TopicPartition, OffsetAndMetadata> = if (specificOffset != null) {
                tps.associateWith { OffsetAndMetadata(specificOffset) }
            } else {
                val spec = if (toLatest) OffsetSpec.latest() else OffsetSpec.earliest()
                admin.listOffsets(tps.associateWith { spec }).all().get()
                    .mapValues { OffsetAndMetadata(it.value.offset()) }
            }

            admin.alterConsumerGroupOffsets(groupId, newOffsets).all().get()
        }
    }

    fun consumeMessages(
        request: ConsumeRequest,
        stopSignal: AtomicBoolean,
        onBatch: (List<KafkaMessageRecord>) -> Unit
    ) = withPluginClassLoader {
        val props = buildBaseProperties(request.cluster).apply {
            put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-ui-debugger-${UUID.randomUUID()}")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer::class.java.name)
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "250")
        }

        KafkaConsumer<ByteArray?, ByteArray?>(props).use { consumer ->
            val partitionInfos = consumer.partitionsFor(request.topic)
                ?: throw IllegalArgumentException("Topic not found: ${request.topic}")

            val targetPartitions = if (request.partition != null) {
                listOf(TopicPartition(request.topic, request.partition))
            } else {
                partitionInfos.map { TopicPartition(request.topic, it.partition()) }
            }

            consumer.assign(targetPartitions)

            val beginningOffsets = consumer.beginningOffsets(targetPartitions)
            val endOffsets = consumer.endOffsets(targetPartitions)

            when (request.mode) {
                ConsumeMode.EARLIEST -> consumer.seekToBeginning(targetPartitions)
                ConsumeMode.LATEST -> consumer.seekToEnd(targetPartitions)
                ConsumeMode.LAST_N -> {
                    val perPartition = ((request.limit + targetPartitions.size - 1) / targetPartitions.size)
                        .coerceAtLeast(1)
                    for (tp in targetPartitions) {
                        val begin = beginningOffsets[tp] ?: 0L
                        val end = endOffsets[tp] ?: 0L
                        val target = (end - perPartition).coerceAtLeast(begin)
                        consumer.seek(tp, target)
                    }
                }
                ConsumeMode.FROM_OFFSET -> {
                    for (tp in targetPartitions) {
                        val begin = beginningOffsets[tp] ?: 0L
                        val end = endOffsets[tp] ?: 0L
                        consumer.seek(tp, request.startOffset.coerceIn(begin, end))
                    }
                }
                ConsumeMode.FROM_TIMESTAMP -> {
                    val timestamps = targetPartitions.associateWith { request.startTimestamp }
                    val found = consumer.offsetsForTimes(timestamps)
                    for (tp in targetPartitions) {
                        val offsetAndTs = found[tp]
                        if (offsetAndTs != null) {
                            consumer.seek(tp, offsetAndTs.offset())
                        } else {
                            consumer.seekToEnd(listOf(tp))
                        }
                    }
                }
            }

            var collected = 0
            var emptyPolls = 0

            while (!stopSignal.get()) {
                val records = consumer.poll(Duration.ofMillis(400))
                if (records.isEmpty) {
                    emptyPolls++
                    if (!request.liveTail) {
                        val reachedEnd = targetPartitions.all { tp ->
                            consumer.position(tp) >= (endOffsets[tp] ?: 0L)
                        }
                        if (reachedEnd || emptyPolls >= 4) break
                    }
                    continue
                }

                emptyPolls = 0
                val batch = records.map { rec ->
                    KafkaMessageRecord(
                        topic = rec.topic(),
                        partition = rec.partition(),
                        offset = rec.offset(),
                        timestamp = rec.timestamp(),
                        keyBytes = rec.key(),
                        valueBytes = rec.value(),
                        headers = rec.headers().map { h -> MessageHeader(h.key(), h.value()) }
                    )
                }

                if (!request.liveTail && collected + batch.size > request.limit) {
                    val remaining = (request.limit - collected).coerceAtLeast(0)
                    if (remaining > 0) {
                        onBatch(batch.take(remaining))
                    }
                    break
                } else {
                    onBatch(batch)
                    collected += batch.size
                    if (!request.liveTail && collected >= request.limit) break
                }
            }
        }
    }

    fun produceMessage(
        cluster: ClusterConfig,
        topic: String,
        partition: Int?,
        keyBytes: ByteArray?,
        valueBytes: ByteArray?,
        headers: List<MessageHeader>
    ): ProducedRecordResult = withPluginClassLoader {
        val props = buildBaseProperties(cluster).apply {
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer::class.java.name)
            put(ProducerConfig.ACKS_CONFIG, "all")
        }

        KafkaProducer<ByteArray?, ByteArray?>(props).use { producer ->
            val kafkaHeaders = headers.map { RecordHeader(it.key, it.value) }
            val record = ProducerRecord(topic, partition, keyBytes, valueBytes, kafkaHeaders)
            val metadata = producer.send(record).get()
            ProducedRecordResult(
                topic = metadata.topic(),
                partition = metadata.partition(),
                offset = metadata.offset(),
                timestamp = metadata.timestamp()
            )
        }
    }
}
