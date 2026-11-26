package dev.edward.kafkadebugger.model

import java.util.*

enum class SecurityProtocol(val kafkaValue: String) {
    PLAINTEXT("PLAINTEXT"),
    SSL("SSL"),
    SASL_PLAINTEXT("SASL_PLAINTEXT"),
    SASL_SSL("SASL_SSL")
}

enum class SaslMechanism(val kafkaValue: String) {
    PLAIN("PLAIN"),
    SCRAM_SHA_256("SCRAM-SHA-256"),
    SCRAM_SHA_512("SCRAM-SHA-512")
}

data class ClusterConfig(
    var id: String = UUID.randomUUID().toString(),
    var name: String = "Local Kafka",
    var bootstrapServers: String = "localhost:9092",
    var securityProtocol: SecurityProtocol = SecurityProtocol.PLAINTEXT,
    var saslMechanism: SaslMechanism = SaslMechanism.PLAIN,
    var saslUsername: String = "",
    var saslPassword: String = "",
    var sslTruststoreLocation: String = "",
    var sslTruststorePassword: String = "",
    var extraProperties: MutableMap<String, String> = mutableMapOf()
) {
    override fun toString(): String = "$name ($bootstrapServers)"
}

data class PartitionInfo(
    val partition: Int,
    val leaderId: Int,
    val replicaIds: List<Int>,
    val isrIds: List<Int>,
    val beginningOffset: Long,
    val endOffset: Long
) {
    val messageCount: Long get() = (endOffset - beginningOffset).coerceAtLeast(0L)
}

data class TopicInfo(
    val name: String,
    val internal: Boolean,
    val partitionCount: Int,
    val replicationFactor: Int,
    val partitions: List<PartitionInfo> = emptyList(),
    val configs: Map<String, String> = emptyMap()
) {
    val totalMessages: Long get() = partitions.sumOf { it.messageCount }
}

data class MessageHeader(
    val key: String,
    val value: ByteArray?
) {
    fun valueAsUtf8(): String = value?.toString(Charsets.UTF_8) ?: "<null>"
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as MessageHeader

        if (key != other.key) return false
        if (!value.contentEquals(other.value)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = key.hashCode()
        result = 31 * result + (value?.contentHashCode() ?: 0)
        return result
    }
}

data class KafkaMessageRecord(
    val topic: String,
    val partition: Int,
    val offset: Long,
    val timestamp: Long,
    val keyBytes: ByteArray?,
    val valueBytes: ByteArray?,
    val headers: List<MessageHeader> = emptyList()
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as KafkaMessageRecord

        if (topic != other.topic) return false
        if (partition != other.partition) return false
        if (offset != other.offset) return false
        if (timestamp != other.timestamp) return false
        if (!keyBytes.contentEquals(other.keyBytes)) return false
        if (!valueBytes.contentEquals(other.valueBytes)) return false
        if (headers != other.headers) return false

        return true
    }

    override fun hashCode(): Int {
        var result = topic.hashCode()
        result = 31 * result + partition
        result = 31 * result + offset.hashCode()
        result = 31 * result + timestamp.hashCode()
        result = 31 * result + (keyBytes?.contentHashCode() ?: 0)
        result = 31 * result + (valueBytes?.contentHashCode() ?: 0)
        result = 31 * result + headers.hashCode()
        return result
    }

    fun keyAsUtf8(): String? = keyBytes?.toString(Charsets.UTF_8)
}

enum class ConsumeMode {
    LAST_N,
    EARLIEST,
    LATEST,
    FROM_OFFSET,
    FROM_TIMESTAMP
}

data class ConsumeRequest(
    val cluster: ClusterConfig,
    val topic: String,
    val partition: Int? = null,
    val mode: ConsumeMode = ConsumeMode.LAST_N,
    val limit: Int = 100,
    val startOffset: Long = 0L,
    val startTimestamp: Long = System.currentTimeMillis() - 3600_000L,
    val liveTail: Boolean = false
)

data class GroupPartitionOffset(
    val topic: String,
    val partition: Int,
    val committedOffset: Long,
    val logEndOffset: Long
) {
    val lag: Long get() = (logEndOffset - committedOffset).coerceAtLeast(0L)
}

data class GroupMemberInfo(
    val memberId: String,
    val clientId: String,
    val host: String,
    val assignments: List<String>
)

data class ConsumerGroupInfo(
    val groupId: String,
    val state: String,
    val coordinator: String,
    val members: List<GroupMemberInfo>,
    val offsets: List<GroupPartitionOffset>
) {
    val totalLag: Long get() = offsets.sumOf { it.lag }
}

enum class ProtoSourceType(val displayName: String) {
    PROTO_DIR(".proto Directory"),
    DESCRIPTOR_SET(".desc / .pb FileDescriptorSet"),
    AVRO_FILE(".avsc Schema File"),
    AVRO_DIR(".avsc Directory")
}

data class ProtoSourceConfig(
    var id: String = UUID.randomUUID().toString(),
    var path: String = "",
    var type: ProtoSourceType = ProtoSourceType.PROTO_DIR
)

data class TopicProtoMapping(
    var topicPattern: String = "",
    var messageFullName: String = "",
    var stripConfluentHeader: Boolean = false
)

data class ConnectionTestResult(
    val success: Boolean,
    val clusterId: String? = null,
    val brokerCount: Int = 0,
    val latencyMs: Long = 0,
    val errorMessage: String? = null
)

data class ProducedRecordResult(
    val topic: String,
    val partition: Int,
    val offset: Long,
    val timestamp: Long
)
