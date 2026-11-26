package dev.edward.kafkadebugger.proto

import com.google.gson.GsonBuilder
import com.google.protobuf.Descriptors
import com.squareup.wire.schema.EnumType
import com.squareup.wire.schema.MessageType
import com.squareup.wire.schema.Schema
import org.apache.avro.Schema as AvroSchema

object SchemaExampleGenerator {

    private val gson = GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create()

    // ──────────────────────────────────────────────────────────────────────────
    // Protobuf (descriptor-based)
    // ──────────────────────────────────────────────────────────────────────────

    fun fromDescriptor(desc: Descriptors.Descriptor, depth: Int = 0): Map<String, Any?> {
        if (depth > 6) return emptyMap()
        val map = linkedMapOf<String, Any?>()
        for (field in desc.fields.sortedBy { it.number }) {
            val value: Any? = when {
                field.isRepeated -> listOf(singleValueForDescriptorField(field, depth))
                else -> singleValueForDescriptorField(field, depth)
            }
            map[field.name] = value
        }
        return map
    }

    private fun singleValueForDescriptorField(field: Descriptors.FieldDescriptor, depth: Int): Any? =
        when (field.type) {
            Descriptors.FieldDescriptor.Type.MESSAGE -> fromDescriptor(field.messageType, depth + 1)
            Descriptors.FieldDescriptor.Type.ENUM -> field.enumType.values.firstOrNull()?.name ?: "UNKNOWN"
            Descriptors.FieldDescriptor.Type.BOOL -> true
            Descriptors.FieldDescriptor.Type.INT32, Descriptors.FieldDescriptor.Type.SINT32,
            Descriptors.FieldDescriptor.Type.SFIXED32, Descriptors.FieldDescriptor.Type.UINT32,
            Descriptors.FieldDescriptor.Type.FIXED32 -> exampleInt(field.name)
            Descriptors.FieldDescriptor.Type.INT64, Descriptors.FieldDescriptor.Type.SINT64,
            Descriptors.FieldDescriptor.Type.SFIXED64, Descriptors.FieldDescriptor.Type.UINT64,
            Descriptors.FieldDescriptor.Type.FIXED64 -> exampleLong(field.name)
            Descriptors.FieldDescriptor.Type.FLOAT -> 1.0f
            Descriptors.FieldDescriptor.Type.DOUBLE -> 1.0
            Descriptors.FieldDescriptor.Type.STRING -> exampleString(field.name)
            Descriptors.FieldDescriptor.Type.BYTES -> ""
            else -> null
        }

    // ──────────────────────────────────────────────────────────────────────────
    // Protobuf (wire-schema based: .proto files)
    // ──────────────────────────────────────────────────────────────────────────

    fun fromWireMessageType(messageFullName: String, schema: Schema, depth: Int = 0): Map<String, Any?> {
        if (depth > 6) return emptyMap()
        val msgType = schema.getType(messageFullName) as? MessageType ?: return emptyMap()
        val map = linkedMapOf<String, Any?>()
        for (field in msgType.fields) {
            val typeName = field.type.toString()
            val value: Any? = if (field.isRepeated) {
                listOf(singleValueForWireField(typeName, field.name, schema, depth))
            } else {
                singleValueForWireField(typeName, field.name, schema, depth)
            }
            map[field.name] = value
        }
        return map
    }

    private fun singleValueForWireField(typeName: String, fieldName: String, schema: Schema, depth: Int): Any? =
        when (typeName) {
            "bool" -> true
            "int32", "sint32", "sfixed32", "uint32", "fixed32" -> exampleInt(fieldName)
            "int64", "sint64", "sfixed64", "uint64", "fixed64" -> exampleLong(fieldName)
            "float" -> 1.0f
            "double" -> 1.0
            "string" -> exampleString(fieldName)
            "bytes" -> ""
            else -> {
                val resolved = schema.getType(typeName)
                when (resolved) {
                    is MessageType -> fromWireMessageType(typeName, schema, depth + 1)
                    is EnumType -> resolved.constants.firstOrNull()?.name ?: "UNKNOWN"
                    else -> exampleString(fieldName)
                }
            }
        }

    // ──────────────────────────────────────────────────────────────────────────
    // Avro
    // ──────────────────────────────────────────────────────────────────────────

    fun fromAvroSchema(schema: AvroSchema, depth: Int = 0): Any? {
        if (depth > 6) return null
        return when (schema.type) {
            AvroSchema.Type.RECORD -> {
                val map = linkedMapOf<String, Any?>()
                for (field in schema.fields) {
                    map[field.name()] = fromAvroSchema(field.schema(), depth + 1)
                }
                map
            }
            AvroSchema.Type.UNION -> {
                // Pick the first non-null type in union
                val chosen = schema.types.firstOrNull { it.type != AvroSchema.Type.NULL }
                if (chosen != null) fromAvroSchema(chosen, depth) else null
            }
            AvroSchema.Type.ARRAY -> listOf(fromAvroSchema(schema.elementType, depth + 1))
            AvroSchema.Type.MAP -> mapOf("key" to fromAvroSchema(schema.valueType, depth + 1))
            AvroSchema.Type.ENUM -> schema.enumSymbols.firstOrNull() ?: "UNKNOWN"
            AvroSchema.Type.BOOLEAN -> true
            AvroSchema.Type.INT -> 1
            AvroSchema.Type.LONG -> 1000L
            AvroSchema.Type.FLOAT -> 1.0f
            AvroSchema.Type.DOUBLE -> 1.0
            AvroSchema.Type.STRING -> exampleString(schema.name ?: "value")
            AvroSchema.Type.BYTES, AvroSchema.Type.FIXED -> ""
            AvroSchema.Type.NULL -> null
        }
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Key generation – produce a sensible string key based on message type name
    // ──────────────────────────────────────────────────────────────────────────

    fun exampleKey(messageFullName: String): String {
        val simple = messageFullName.substringAfterLast('.')
        return when {
            simple.contains("Order", ignoreCase = true) -> "order-${System.currentTimeMillis() % 10000}"
            simple.contains("User", ignoreCase = true) -> "user-001"
            simple.contains("Product", ignoreCase = true) -> "product-001"
            simple.contains("Payment", ignoreCase = true) -> "payment-${System.currentTimeMillis() % 10000}"
            simple.contains("Event", ignoreCase = true) -> "event-001"
            simple.contains("Message", ignoreCase = true) -> "msg-001"
            else -> "${simple.lowercase()}-001"
        }
    }

    fun toJson(value: Any?): String = gson.toJson(value)

    // ──────────────────────────────────────────────────────────────────────────
    // Heuristic value generation helpers
    // ──────────────────────────────────────────────────────────────────────────

    private fun exampleString(name: String): String = when {
        name.contains("id", ignoreCase = true) -> "sample-id-001"
        name.contains("name", ignoreCase = true) -> "Sample Name"
        name.contains("email", ignoreCase = true) -> "user@example.com"
        name.contains("phone", ignoreCase = true) -> "+84900000000"
        name.contains("url", ignoreCase = true) || name.contains("link", ignoreCase = true) -> "https://example.com"
        name.contains("status", ignoreCase = true) -> "ACTIVE"
        name.contains("type", ignoreCase = true) -> "DEFAULT"
        name.contains("code", ignoreCase = true) -> "CODE_001"
        name.contains("desc", ignoreCase = true) || name.contains("note", ignoreCase = true) -> "Sample description"
        name.contains("address", ignoreCase = true) -> "123 Main Street"
        name.contains("city", ignoreCase = true) -> "Ho Chi Minh City"
        name.contains("country", ignoreCase = true) -> "VN"
        name.contains("currency", ignoreCase = true) -> "VND"
        name.contains("token", ignoreCase = true) || name.contains("key", ignoreCase = true) -> "sample-token-abc"
        else -> "sample-$name"
    }

    private fun exampleInt(name: String): Int = when {
        name.contains("age", ignoreCase = true) -> 25
        name.contains("count", ignoreCase = true) || name.contains("qty", ignoreCase = true) -> 1
        name.contains("port", ignoreCase = true) -> 8080
        name.contains("score", ignoreCase = true) -> 100
        name.contains("page", ignoreCase = true) -> 1
        name.contains("size", ignoreCase = true) -> 10
        else -> 1
    }

    private fun exampleLong(name: String): Long = when {
        name.contains("time", ignoreCase = true) || name.contains("at", ignoreCase = true)
                || name.contains("date", ignoreCase = true) || name.contains("ts", ignoreCase = true) ->
            System.currentTimeMillis()
        name.contains("amount", ignoreCase = true) || name.contains("price", ignoreCase = true)
                || name.contains("cents", ignoreCase = true) -> 100000L
        else -> 1L
    }
}
