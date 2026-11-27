package dev.edward.kafkadebugger.proto

import com.google.gson.GsonBuilder
import com.google.gson.JsonParser
import com.google.protobuf.*
import com.google.protobuf.util.JsonFormat
import com.intellij.openapi.components.Service
import com.intellij.openapi.project.Project
import com.squareup.wire.schema.Location
import com.squareup.wire.schema.MessageType
import com.squareup.wire.schema.Schema
import com.squareup.wire.schema.SchemaLoader
import dev.edward.kafkadebugger.model.ProtoSourceConfig
import dev.edward.kafkadebugger.model.ProtoSourceType
import okio.ByteString
import okio.ByteString.Companion.toByteString
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayOutputStream
import java.io.File
import java.nio.ByteBuffer
import java.nio.file.FileSystems
import org.apache.avro.Schema as AvroSchema

@Service(Service.Level.PROJECT)
class ProtoRegistryService {

    private val gson = GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create()
    private val jsonPrinter = JsonFormat.printer()
        .includingDefaultValueFields()
        .preservingProtoFieldNames()
    private val jsonParser = JsonFormat.parser().ignoringUnknownFields()

    @Volatile
    private var descriptorMap: Map<String, Descriptors.Descriptor> = emptyMap()

    @Volatile
    private var wireSchemas: List<Schema> = emptyList()

    @Volatile
    private var wireMessageNames: Set<String> = emptySet()

    @Volatile
    private var avroSchemas: Map<String, AvroSchema> = emptyMap()

    @Volatile
    private var lastLoadErrors: List<String> = emptyList()

    fun reload(sources: List<ProtoSourceConfig>): List<String> {
        val errors = mutableListOf<String>()
        val newDescriptors = linkedMapOf<String, Descriptors.Descriptor>()
        val newWireSchemas = mutableListOf<Schema>()
        val newWireNames = linkedSetOf<String>()
        val newAvroSchemas = linkedMapOf<String, AvroSchema>()

        for ((_, path, type) in sources) {
            val file = File(path.trim())
            if (!file.exists()) {
                errors.add("Path not found: $path")
                continue
            }
            when (type) {
                ProtoSourceType.DESCRIPTOR_SET -> {
                    try {
                        loadDescriptorSet(file, newDescriptors)
                    } catch (e: Exception) {
                        errors.add("Failed to load descriptor ${file.name}: ${e.message}")
                    }
                }

                ProtoSourceType.PROTO_DIR -> {
                    try {
                        val loader = SchemaLoader(FileSystems.getDefault())
                        loader.loadExhaustively = true
                        loader.initRoots(listOf(Location.get(file.absolutePath)), emptyList())
                        val schema = loader.loadSchema()
                        newWireSchemas.add(schema)
                        for (t in schema.types) {
                            if (schema.getType(t) is MessageType) {
                                newWireNames.add(t.toString())
                            }
                        }
                    } catch (e: Exception) {
                        errors.add("Failed to load .proto dir ${file.name}: ${e.message}")
                    }
                }

                ProtoSourceType.AVRO_FILE -> {
                    try {
                        val schema = AvroSchema.Parser().parse(file)
                        registerAvroSchema(schema, newAvroSchemas)
                    } catch (e: Exception) {
                        errors.add("Failed to load Avro schema ${file.name}: ${e.message}")
                    }
                }

                ProtoSourceType.AVRO_DIR -> {
                    val avscFiles = file.walkTopDown()
                        .filter { it.isFile && it.extension.equals("avsc", ignoreCase = true) }
                        .sortedBy { it.absolutePath }
                        .toList()
                    val sharedParser = AvroSchema.Parser()
                    for (avsc in avscFiles) {
                        try {
                            val schema = sharedParser.parse(avsc)
                            registerAvroSchema(schema, newAvroSchemas)
                        } catch (_: Exception) {
                            try {
                                val isolated = AvroSchema.Parser().parse(avsc)
                                registerAvroSchema(isolated, newAvroSchemas)
                            } catch (e: Exception) {
                                errors.add("Failed to load Avro ${avsc.name}: ${e.message}")
                            }
                        }
                    }
                }
            }
        }

        descriptorMap = newDescriptors
        wireSchemas = newWireSchemas
        wireMessageNames = newWireNames
        avroSchemas = newAvroSchemas
        lastLoadErrors = errors
        return errors
    }

    fun getAvailableMessageTypes(): List<String> {
        return (descriptorMap.keys + wireMessageNames + avroSchemas.keys).distinct().sorted()
    }

    fun isAvroType(messageFullName: String?): Boolean =
        messageFullName != null && avroSchemas.containsKey(messageFullName)

    /**
     * Generate an example (key, payloadJson) pair for the given message type.
     * Returns null if no matching schema is loaded.
     */
    fun generateExample(messageFullName: String): Pair<String, String>? {
        val key = SchemaExampleGenerator.exampleKey(messageFullName)

        // Avro
        val avroSchema = avroSchemas[messageFullName]
        if (avroSchema != null) {
            val exampleValue = SchemaExampleGenerator.fromAvroSchema(avroSchema)
            return key to SchemaExampleGenerator.toJson(exampleValue)
        }

        // Protobuf descriptor (.desc / .pb)
        val desc = descriptorMap[messageFullName]
        if (desc != null) {
            val exampleMap = SchemaExampleGenerator.fromDescriptor(desc)
            return key to SchemaExampleGenerator.toJson(exampleMap)
        }

        // Protobuf .proto dir (wire-schema)
        val schema = wireSchemas.firstOrNull { it.getType(messageFullName) is MessageType }
        if (schema != null) {
            val exampleMap = SchemaExampleGenerator.fromWireMessageType(messageFullName, schema)
            return key to SchemaExampleGenerator.toJson(exampleMap)
        }

        return null
    }

    fun decode(
        bytes: ByteArray?,
        messageFullName: String? = null,
        stripConfluentHeader: Boolean = false
    ): DecodedPayload {
        if (bytes == null) {
            return DecodedPayload(
                prettyJson = "<null>",
                rawUtf8 = "<null>",
                hexDump = "<null>",
                decoderUsed = "Null"
            )
        }

        val rawUtf8 = bytes.toString(Charsets.UTF_8)
        val hexDump = HexDumpFormatter.format(bytes)

        var payload = bytes
        var confluentSchemaId: Int? = null
        val isAvro = isAvroType(messageFullName)
        if (stripConfluentHeader) {
            if (isAvro) {
                val avroFrame = ConfluentWireFormat.tryStripAvroHeader(bytes)
                if (avroFrame != null) {
                    confluentSchemaId = avroFrame.first
                    payload = avroFrame.second
                }
            } else {
                val frame = ConfluentWireFormat.tryStripHeader(bytes)
                if (frame != null) {
                    payload = frame.payload
                    confluentSchemaId = frame.schemaId
                }
            }
        }

        if (!messageFullName.isNullOrBlank()) {
            // Try direct decode first; if it fails and Confluent header wasn't stripped yet, try stripping automatically
            val direct = tryDecodeWithSchema(payload, messageFullName, rawUtf8, hexDump, confluentSchemaId)
            if (direct != null) return direct

            if (!stripConfluentHeader) {
                if (isAvro) {
                    val avroFrame = ConfluentWireFormat.tryStripAvroHeader(bytes)
                    if (avroFrame != null) {
                        val stripped = tryDecodeWithSchema(
                            avroFrame.second, messageFullName, rawUtf8, hexDump, avroFrame.first
                        )
                        if (stripped != null) return stripped
                    }
                } else {
                    val frame = ConfluentWireFormat.tryStripHeader(bytes)
                    if (frame != null) {
                        val stripped = tryDecodeWithSchema(
                            frame.payload, messageFullName, rawUtf8, hexDump, frame.schemaId
                        )
                        if (stripped != null) return stripped
                    }
                }
            }

            // Fallback to raw wire if schema decode failed
            val rawProto = RawProtobufDecoder.tryDecode(payload)
            return DecodedPayload(
                prettyJson = rawProto?.prettyJson ?: rawUtf8,
                rawUtf8 = rawUtf8,
                hexDump = hexDump,
                decoderUsed = "Fallback (Failed $messageFullName)",
                treeRoot = rawProto?.treeRoot,
                confluentSchemaId = confluentSchemaId,
                errorNote = "Could not decode bytes as $messageFullName; showing fallback view."
            )
        }

        // Auto-detect without explicit message type
        val trimmed = rawUtf8.trim()
        if ((trimmed.startsWith("{") && trimmed.endsWith("}")) ||
            (trimmed.startsWith("[") && trimmed.endsWith("]"))
        ) {
            try {
                val parsed = JsonParser.parseString(trimmed)
                return DecodedPayload(
                    prettyJson = gson.toJson(parsed),
                    rawUtf8 = rawUtf8,
                    hexDump = hexDump,
                    decoderUsed = "JSON"
                )
            } catch (_: Exception) {
                // Not valid JSON, continue
            }
        }

        if (isPrintableText(bytes)) {
            return DecodedPayload(
                prettyJson = rawUtf8,
                rawUtf8 = rawUtf8,
                hexDump = hexDump,
                decoderUsed = "UTF-8 Text"
            )
        }

        val rawProto = RawProtobufDecoder.tryDecode(payload)
        if (rawProto != null) {
            return DecodedPayload(
                prettyJson = rawProto.prettyJson,
                rawUtf8 = rawUtf8,
                hexDump = hexDump,
                decoderUsed = "Protobuf (Schemaless Raw Wire)",
                treeRoot = rawProto.treeRoot,
                confluentSchemaId = confluentSchemaId
            )
        }

        return DecodedPayload(
            prettyJson = hexDump,
            rawUtf8 = rawUtf8,
            hexDump = hexDump,
            decoderUsed = "Binary / Hex"
        )
    }

    fun encodeJsonToProtobuf(
        json: String,
        messageFullName: String,
        confluentSchemaId: Int? = null
    ): ByteArray = encodeJsonToSchema(json, messageFullName, confluentSchemaId)

    fun encodeJsonToSchema(
        json: String,
        messageFullName: String,
        confluentSchemaId: Int? = null
    ): ByteArray {
        val avroSchema = avroSchemas[messageFullName]
        if (avroSchema != null) {
            // Convert standard JSON (plain string values) → GenericRecord via Map,
            // so users don't have to use Avro union-encoded JSON format {"string":"value"}.
            @Suppress("UNCHECKED_CAST")
            val rawMap = gson.fromJson(json, Map::class.java) as? Map<String, Any?>
                ?: throw IllegalArgumentException("Avro payload must be a JSON object")
            val record = jsonMapToGenericRecord(avroSchema, rawMap)
            val out = ByteArrayOutputStream()
            val encoder = EncoderFactory.get().binaryEncoder(out, null)
            GenericDatumWriter<Any>(avroSchema).write(record, encoder)
            encoder.flush()
            val rawAvro = out.toByteArray()
            return if (confluentSchemaId != null && confluentSchemaId > 0) {
                ConfluentWireFormat.prependAvroHeader(confluentSchemaId, rawAvro)
            } else {
                rawAvro
            }
        }

        val desc = descriptorMap[messageFullName]
        val rawBytes = if (desc != null) {
            val builder = DynamicMessage.newBuilder(desc)
            jsonParser.merge(json, builder)
            builder.build().toByteArray()
        } else {
            val schema = wireSchemas.firstOrNull { it.getType(messageFullName) is MessageType }
                ?: throw IllegalArgumentException("Message type not found in loaded schemas: $messageFullName")
            val msgType = schema.getType(messageFullName) as MessageType

            @Suppress("UNCHECKED_CAST")
            val rawMap = gson.fromJson(json, Map::class.java) as? Map<String, Any?>
                ?: throw IllegalArgumentException("Payload must be a JSON object")
            val coerced = coerceMapForWire(rawMap, msgType, schema)
            val adapter = schema.protoAdapter(messageFullName, true)
            adapter.encode(coerced)
        }

        return if (confluentSchemaId != null && confluentSchemaId > 0) {
            ConfluentWireFormat.prependHeader(confluentSchemaId, rawBytes)
        } else {
            rawBytes
        }
    }

    private fun tryDecodeWithSchema(
        payload: ByteArray,
        messageFullName: String,
        rawUtf8: String,
        hexDump: String,
        confluentSchemaId: Int?
    ): DecodedPayload? {
        val avroSchema = avroSchemas[messageFullName]
        if (avroSchema != null) {
            return try {
                val decoder = DecoderFactory.get().binaryDecoder(payload, null)
                val reader = GenericDatumReader<Any>(avroSchema)
                val datum = reader.read(null, decoder)
                val jsonFriendly = avroValueToJsonFriendly(datum)
                val tree = buildAvroTree(avroSchema.name, avroSchema, datum)
                val suffix = if (confluentSchemaId != null) " [Confluent #$confluentSchemaId]" else ""
                DecodedPayload(
                    prettyJson = gson.toJson(jsonFriendly),
                    rawUtf8 = rawUtf8,
                    hexDump = hexDump,
                    decoderUsed = "Avro (${avroSchema.fullName})$suffix",
                    treeRoot = tree,
                    confluentSchemaId = confluentSchemaId
                )
            } catch (_: Exception) {
                null
            }
        }
        val desc = descriptorMap[messageFullName]
        if (desc != null) {
            return try {
                val msg = DynamicMessage.parseFrom(desc, payload)
                val json = jsonPrinter.print(msg)
                val tree = buildDynamicMessageTree(desc.name, msg)
                val suffix = if (confluentSchemaId != null) " [Confluent #$confluentSchemaId]" else ""
                DecodedPayload(
                    prettyJson = json,
                    rawUtf8 = rawUtf8,
                    hexDump = hexDump,
                    decoderUsed = "Protobuf (${desc.fullName})$suffix",
                    treeRoot = tree,
                    confluentSchemaId = confluentSchemaId
                )
            } catch (_: Exception) {
                null
            }
        }

        val schema = wireSchemas.firstOrNull { it.getType(messageFullName) is MessageType }
        if (schema != null) {
            return try {
                val adapter = schema.protoAdapter(messageFullName, true)

                @Suppress("UNCHECKED_CAST")
                val decoded = adapter.decode(payload) as Map<String, Any?>
                val jsonReady = wireMapToJsonFriendly(decoded)
                val tree = buildWireTree(messageFullName.substringAfterLast('.'), messageFullName, decoded)
                val suffix = if (confluentSchemaId != null) " [Confluent #$confluentSchemaId]" else ""
                DecodedPayload(
                    prettyJson = gson.toJson(jsonReady),
                    rawUtf8 = rawUtf8,
                    hexDump = hexDump,
                    decoderUsed = "Protobuf ($messageFullName)$suffix",
                    treeRoot = tree,
                    confluentSchemaId = confluentSchemaId
                )
            } catch (_: Exception) {
                null
            }
        }
        return null
    }

    private fun loadDescriptorSet(file: File, target: MutableMap<String, Descriptors.Descriptor>) {
        val fds = DescriptorProtos.FileDescriptorSet.parseFrom(file.readBytes())
        val protoMap = fds.fileList.associateBy { it.name }
        val built = mutableMapOf<String, Descriptors.FileDescriptor>()

        fun buildRecursive(name: String): Descriptors.FileDescriptor? {
            built[name]?.let { return it }
            wellKnownDescriptor(name)?.let {
                built[name] = it
                return it
            }
            val proto = protoMap[name] ?: return null
            val deps = proto.dependencyList.mapNotNull { buildRecursive(it) }.toTypedArray()
            val fd = Descriptors.FileDescriptor.buildFrom(proto, deps)
            built[name] = fd
            return fd
        }

        for (fp in fds.fileList) {
            val fd = buildRecursive(fp.name) ?: continue
            fun collectMessages(types: List<Descriptors.Descriptor>) {
                for (t in types) {
                    target[t.fullName] = t
                    collectMessages(t.nestedTypes)
                }
            }
            collectMessages(fd.messageTypes)
        }
    }

    private fun wellKnownDescriptor(name: String): Descriptors.FileDescriptor? = when (name) {
        "google/protobuf/timestamp.proto" -> TimestampProto.getDescriptor()
        "google/protobuf/duration.proto" -> DurationProto.getDescriptor()
        "google/protobuf/any.proto" -> AnyProto.getDescriptor()
        "google/protobuf/empty.proto" -> EmptyProto.getDescriptor()
        "google/protobuf/struct.proto" -> StructProto.getDescriptor()
        "google/protobuf/wrappers.proto" -> WrappersProto.getDescriptor()
        "google/protobuf/field_mask.proto" -> FieldMaskProto.getDescriptor()
        else -> null
    }

    private fun buildDynamicMessageTree(label: String, msg: Message): ProtoTreeNode {
        val children = msg.allFields.entries
            .sortedBy { it.key.number }
            .map { (field, value) ->
                buildFieldNode(field, value)
            }
        return ProtoTreeNode(
            label = label,
            typeName = msg.descriptorForType.fullName,
            value = "${children.size} fields",
            children = children
        )
    }

    private fun buildFieldNode(field: Descriptors.FieldDescriptor, value: Any): ProtoTreeNode {
        if (field.isRepeated && value is List<*>) {
            val itemNodes = value.mapIndexed { idx, item ->
                if (item is Message) {
                    buildDynamicMessageTree("[$idx]", item)
                } else {
                    ProtoTreeNode("[$idx]", field.type.name.lowercase(), item.toString())
                }
            }
            return ProtoTreeNode(
                label = field.name,
                typeName = "repeated ${field.type.name.lowercase()}",
                value = "[${value.size} items]",
                fieldNumber = field.number,
                children = itemNodes
            )
        }
        return when (value) {
            is Message -> {
                val sub = buildDynamicMessageTree(field.name, value)
                sub.copy(fieldNumber = field.number)
            }

            is com.google.protobuf.ByteString -> ProtoTreeNode(
                label = field.name,
                typeName = "bytes",
                value = "${value.size()} bytes",
                fieldNumber = field.number
            )

            else -> ProtoTreeNode(
                label = field.name,
                typeName = field.type.name.lowercase(),
                value = value.toString(),
                fieldNumber = field.number
            )
        }
    }

    private fun buildWireTree(label: String, typeName: String, map: Map<String, Any?>): ProtoTreeNode {
        val children = map.entries.map { (k, v) ->
            when (v) {
                is Map<*, *> -> {
                    @Suppress("UNCHECKED_CAST")
                    buildWireTree(k, "message", v as Map<String, Any?>)
                }

                is List<*> -> {
                    val items = v.mapIndexed { idx, item ->
                        if (item is Map<*, *>) {
                            @Suppress("UNCHECKED_CAST")
                            buildWireTree("[$idx]", "message", item as Map<String, Any?>)
                        } else {
                            ProtoTreeNode("[$idx]", item?.javaClass?.simpleName ?: "any", item.toString())
                        }
                    }
                    ProtoTreeNode(k, "repeated", "[${v.size} items]", children = items)
                }

                is ByteString -> ProtoTreeNode(k, "bytes", "${v.size} bytes")
                else -> ProtoTreeNode(k, v?.javaClass?.simpleName ?: "null", v.toString())
            }
        }
        return ProtoTreeNode(label, typeName, "${children.size} fields", children = children)
    }

    private fun wireMapToJsonFriendly(map: Map<String, Any?>): Map<String, Any?> {
        val out = linkedMapOf<String, Any?>()
        for ((k, v) in map) {
            out[k] = wireValueToJsonFriendly(v)
        }
        return out
    }

    private fun wireValueToJsonFriendly(v: Any?): Any? = when (v) {
        is Map<*, *> -> {
            val sub = linkedMapOf<String, Any?>()
            for ((mk, mv) in v) {
                sub[mk.toString()] = wireValueToJsonFriendly(mv)
            }
            sub
        }

        is List<*> -> v.map { wireValueToJsonFriendly(it) }
        is ByteString -> v.base64()
        else -> v
    }

    private fun coerceMapForWire(
        rawMap: Map<String, Any?>,
        msgType: MessageType,
        schema: Schema
    ): Map<String, Any?> {
        val out = linkedMapOf<String, Any?>()
        for ((k, v) in rawMap) {
            if (v == null) continue
            val field = msgType.field(k) ?: msgType.fields.firstOrNull { it.name == k } ?: continue
            val fieldType = field.type
            if (field.isRepeated && v is List<*>) {
                out[field.name] = v.mapNotNull { item -> coerceValueForWire(item, fieldType.toString(), schema) }
            } else {
                out[field.name] = coerceValueForWire(v, fieldType.toString(), schema)
            }
        }
        return out
    }

    private fun coerceValueForWire(v: Any?, typeName: String, schema: Schema): Any? {
        if (v == null) return null
        return when (typeName) {
            "int32", "sint32", "sfixed32" -> (v as? Number)?.toInt() ?: v.toString().toInt()
            "int64", "sint64", "sfixed64", "uint64", "fixed64" -> (v as? Number)?.toLong() ?: v.toString().toLong()
            "uint32", "fixed32" -> (v as? Number)?.toInt() ?: v.toString().toInt()
            "float" -> (v as? Number)?.toFloat() ?: v.toString().toFloat()
            "double" -> (v as? Number)?.toDouble() ?: v.toString().toDouble()
            "bool" -> (v as? Boolean) ?: v.toString().toBoolean()
            "string" -> v.toString()
            "bytes" -> v.toString().encodeToByteArray().toByteString()
            else -> {
                val targetType = schema.getType(typeName)
                if (targetType is MessageType && v is Map<*, *>) {
                    @Suppress("UNCHECKED_CAST")
                    coerceMapForWire(v as Map<String, Any?>, targetType, schema)
                } else {
                    v.toString()
                }
            }
        }
    }

    private fun jsonMapToGenericRecord(schema: AvroSchema, map: Map<String, Any?>): GenericRecord {
        val record = org.apache.avro.generic.GenericData.Record(schema)
        for (field in schema.fields) {
            val rawValue = map[field.name()]
            record.put(field.name(), coerceToAvro(rawValue, field.schema()))
        }
        return record
    }

    private fun coerceToAvro(value: Any?, schema: AvroSchema): Any? {
        return when (schema.type) {
            AvroSchema.Type.UNION -> {
                if (value == null) {
                    return if (schema.types.any { it.type == AvroSchema.Type.NULL }) null
                    else coerceToAvro(null, schema.types.first())
                }
                for (branch in schema.types.filter { it.type != AvroSchema.Type.NULL }) {
                    try {
                        return coerceToAvro(value, branch)
                    } catch (_: Exception) {
                    }
                }
                null
            }

            AvroSchema.Type.RECORD -> {
                @Suppress("UNCHECKED_CAST")
                val map = value as? Map<String, Any?> ?: emptyMap()
                jsonMapToGenericRecord(schema, map)
            }

            AvroSchema.Type.ARRAY -> {
                val list = when (value) {
                    is List<*> -> value
                    null -> emptyList()
                    else -> listOf(value)
                }
                val avroList = org.apache.avro.generic.GenericData.Array<Any?>(list.size, schema)
                list.forEach { avroList.add(coerceToAvro(it, schema.elementType)) }
                avroList
            }

            AvroSchema.Type.MAP -> {
                val m = value as? Map<*, *> ?: emptyMap<String, Any?>()
                m.entries.associate { (k, v) -> k.toString() to coerceToAvro(v, schema.valueType) }
            }

            AvroSchema.Type.ENUM -> {
                org.apache.avro.generic.GenericData.EnumSymbol(schema, value?.toString() ?: schema.enumSymbols.first())
            }

            AvroSchema.Type.FIXED -> {
                val bytes = value?.toString()?.toByteArray(Charsets.UTF_8) ?: ByteArray(schema.fixedSize)
                org.apache.avro.generic.GenericData.Fixed(schema, bytes.copyOf(schema.fixedSize))
            }

            AvroSchema.Type.STRING -> value?.toString() ?: ""
            AvroSchema.Type.BYTES -> ByteBuffer.wrap(value?.toString()?.toByteArray(Charsets.UTF_8) ?: ByteArray(0))
            AvroSchema.Type.INT -> when (value) {
                is Number -> value.toInt()
                else -> value?.toString()?.toIntOrNull() ?: 0
            }

            AvroSchema.Type.LONG -> when (value) {
                is Number -> value.toLong()
                else -> value?.toString()?.toLongOrNull() ?: 0L
            }

            AvroSchema.Type.FLOAT -> when (value) {
                is Number -> value.toFloat()
                else -> value?.toString()?.toFloatOrNull() ?: 0f
            }

            AvroSchema.Type.DOUBLE -> when (value) {
                is Number -> value.toDouble()
                else -> value?.toString()?.toDoubleOrNull() ?: 0.0
            }

            AvroSchema.Type.BOOLEAN -> when (value) {
                is Boolean -> value
                else -> value?.toString()?.toBooleanStrictOrNull() ?: false
            }

            AvroSchema.Type.NULL -> null
        }
    }

    private fun registerAvroSchema(schema: AvroSchema, target: MutableMap<String, AvroSchema>) {
        when (schema.type) {
            AvroSchema.Type.RECORD -> target[schema.fullName] = schema
            AvroSchema.Type.UNION -> schema.types.forEach { registerAvroSchema(it, target) }
            else -> Unit
        }
    }

    private fun avroValueToJsonFriendly(v: Any?): Any? = when (v) {
        null -> null
        is GenericRecord -> {
            val map = linkedMapOf<String, Any?>()
            for (field in v.schema.fields) {
                map[field.name()] = avroValueToJsonFriendly(v.get(field.name()))
            }
            map
        }

        is Collection<*> -> v.map { avroValueToJsonFriendly(it) }
        is Map<*, *> -> {
            val map = linkedMapOf<String, Any?>()
            for ((mk, mv) in v) {
                map[mk.toString()] = avroValueToJsonFriendly(mv)
            }
            map
        }

        is ByteBuffer -> {
            val dup = v.duplicate()
            val bytes = ByteArray(dup.remaining())
            dup.get(bytes)
            bytes.toByteString().base64()
        }

        is org.apache.avro.generic.GenericFixed -> v.bytes().toByteString().base64()
        is CharSequence, is Enum<*>, is org.apache.avro.generic.GenericEnumSymbol<*> -> v.toString()
        else -> v
    }

    private fun buildAvroTree(label: String, schema: AvroSchema, value: Any?): ProtoTreeNode {
        if (value == null) {
            return ProtoTreeNode(label, schema.type.getName(), "null")
        }
        return when (value) {
            is GenericRecord -> {
                val children = value.schema.fields.map { f ->
                    buildAvroTree(f.name(), f.schema(), value.get(f.name()))
                }
                ProtoTreeNode(label, value.schema.fullName, "${children.size} fields", children = children)
            }

            is Collection<*> -> {
                val itemSchema = if (schema.type == AvroSchema.Type.ARRAY) schema.elementType else schema
                val children = value.mapIndexed { idx, item ->
                    buildAvroTree("[$idx]", itemSchema, item)
                }
                ProtoTreeNode(label, "array", "[${value.size} items]", children = children)
            }

            is Map<*, *> -> {
                val valSchema = if (schema.type == AvroSchema.Type.MAP) schema.valueType else schema
                val children = value.entries.map { (mk, mv) ->
                    buildAvroTree(mk.toString(), valSchema, mv)
                }
                ProtoTreeNode(label, "map", "{${value.size} entries}", children = children)
            }

            is ByteBuffer -> ProtoTreeNode(label, "bytes", "${value.remaining()} bytes")
            else -> ProtoTreeNode(label, schema.type.getName(), value.toString())
        }
    }

    private fun isPrintableText(bytes: ByteArray): Boolean {
        if (bytes.isEmpty()) return true
        val str = bytes.toString(Charsets.UTF_8)
        return !str.contains('\uFFFD') && str.all { ch -> !ch.isISOControl() || ch == '\n' || ch == '\r' || ch == '\t' }
    }

    companion object {
        fun getInstance(project: Project): ProtoRegistryService =
            project.getService(ProtoRegistryService::class.java)
    }
}
