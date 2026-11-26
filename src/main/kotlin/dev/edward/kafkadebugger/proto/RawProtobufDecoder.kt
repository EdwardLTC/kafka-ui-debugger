package dev.edward.kafkadebugger.proto

import com.google.gson.GsonBuilder
import com.google.protobuf.CodedInputStream
import com.google.protobuf.WireFormat

object RawProtobufDecoder {
    private val gson = GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create()

    data class RawDecodeResult(
        val treeRoot: ProtoTreeNode,
        val prettyJson: String
    )

    fun tryDecode(bytes: ByteArray, maxDepth: Int = 8): RawDecodeResult? {
        if (bytes.isEmpty()) return null
        return try {
            val fields = parseFields(bytes, 0, maxDepth)
            if (fields.isEmpty()) return null
            val root = ProtoTreeNode(
                label = "Message (Schemaless)",
                typeName = "raw_protobuf",
                value = "${fields.size} fields",
                children = fields.map { it.node }
            )
            val jsonMap = fieldsToMap(fields)
            RawDecodeResult(root, gson.toJson(jsonMap))
        } catch (_: Exception) {
            null
        }
    }

    private data class ParsedField(
        val number: Int,
        val node: ProtoTreeNode,
        val jsonValue: Any?
    )

    private fun parseFields(bytes: ByteArray, depth: Int, maxDepth: Int): List<ParsedField> {
        val input = CodedInputStream.newInstance(bytes)
        val result = mutableListOf<ParsedField>()
        while (!input.isAtEnd) {
            val tag = input.readTag()
            if (tag == 0) break
            val fieldNumber = WireFormat.getTagFieldNumber(tag)
            if (fieldNumber !in 1..536_870_911) {
                throw IllegalArgumentException("Invalid field number: $fieldNumber")
            }
            when (val wireType = WireFormat.getTagWireType(tag)) {
                WireFormat.WIRETYPE_VARINT -> {
                    val v = input.readInt64()
                    result.add(
                        ParsedField(
                            fieldNumber,
                            ProtoTreeNode("field_$fieldNumber", "varint", v.toString(), fieldNumber),
                            v
                        )
                    )
                }

                WireFormat.WIRETYPE_FIXED64 -> {
                    val v = input.readFixed64()
                    val asDouble = java.lang.Double.longBitsToDouble(v)
                    val display = "$v (double=$asDouble)"
                    result.add(
                        ParsedField(
                            fieldNumber,
                            ProtoTreeNode("field_$fieldNumber", "fixed64", display, fieldNumber),
                            v
                        )
                    )
                }

                WireFormat.WIRETYPE_FIXED32 -> {
                    val v = input.readFixed32()
                    val asFloat = java.lang.Float.intBitsToFloat(v)
                    val display = "$v (float=$asFloat)"
                    result.add(
                        ParsedField(
                            fieldNumber,
                            ProtoTreeNode("field_$fieldNumber", "fixed32", display, fieldNumber),
                            v
                        )
                    )
                }

                WireFormat.WIRETYPE_LENGTH_DELIMITED -> {
                    val subBytes = input.readByteArray()
                    val nested = if (depth < maxDepth && subBytes.isNotEmpty()) {
                        try {
                            val subFields = parseFields(subBytes, depth + 1, maxDepth)
                            subFields.ifEmpty { null }
                        } catch (_: Exception) {
                            null
                        }
                    } else null

                    if (nested != null && !isLikelyPlainText(subBytes)) {
                        result.add(
                            ParsedField(
                                fieldNumber,
                                ProtoTreeNode(
                                    "field_$fieldNumber",
                                    "message",
                                    "${nested.size} fields",
                                    fieldNumber,
                                    nested.map { it.node }
                                ),
                                fieldsToMap(nested)
                            )
                        )
                    } else if (isLikelyPlainText(subBytes)) {
                        val str = subBytes.toString(Charsets.UTF_8)
                        result.add(
                            ParsedField(
                                fieldNumber,
                                ProtoTreeNode("field_$fieldNumber", "string", str, fieldNumber),
                                str
                            )
                        )
                    } else {
                        val hex = subBytes.joinToString(" ") { String.format("%02X", it.toInt() and 0xFF) }
                        result.add(
                            ParsedField(
                                fieldNumber,
                                ProtoTreeNode("field_$fieldNumber", "bytes[${subBytes.size}]", hex, fieldNumber),
                                "0x$hex"
                            )
                        )
                    }
                }

                WireFormat.WIRETYPE_START_GROUP -> {
                    input.skipField(tag)
                }

                else -> throw IllegalArgumentException("Unsupported wire type: $wireType")
            }
        }
        return result
    }

    private fun fieldsToMap(fields: List<ParsedField>): Map<String, Any?> {
        val map = linkedMapOf<String, Any?>()
        for ((number, _, jsonValue) in fields) {
            val key = "field_$number"
            when (val existing = map[key]) {
                null -> {
                    map[key] = jsonValue
                }

                is MutableList<*> -> {
                    @Suppress("UNCHECKED_CAST")
                    (existing as MutableList<Any?>).add(jsonValue)
                }

                else -> {
                    map[key] = mutableListOf(existing, jsonValue)
                }
            }
        }
        return map
    }

    private fun isLikelyPlainText(bytes: ByteArray): Boolean {
        if (bytes.isEmpty()) return true
        val str = bytes.toString(Charsets.UTF_8)
        return !str.contains('\uFFFD') && str.all { ch -> !ch.isISOControl() || ch == '\n' || ch == '\r' || ch == '\t' }
    }
}
