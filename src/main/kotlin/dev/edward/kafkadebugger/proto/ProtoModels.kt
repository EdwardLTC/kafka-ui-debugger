package dev.edward.kafkadebugger.proto

data class ProtoTreeNode(
    val label: String,
    val typeName: String,
    val value: String,
    val fieldNumber: Int? = null,
    val children: List<ProtoTreeNode> = emptyList()
) {
    override fun toString(): String {
        val prefix = if (fieldNumber != null) "#$fieldNumber " else ""
        return if (children.isEmpty()) {
            "$prefix$label ($typeName): $value"
        } else {
            "$prefix$label ($typeName)"
        }
    }
}

data class DecodedPayload(
    val prettyJson: String,
    val rawUtf8: String,
    val hexDump: String,
    val decoderUsed: String,
    val treeRoot: ProtoTreeNode? = null,
    val confluentSchemaId: Int? = null,
    val errorNote: String? = null
)

object HexDumpFormatter {
    fun format(bytes: ByteArray?, maxBytes: Int = 16384): String {
        if (bytes == null) return "<null>"
        if (bytes.isEmpty()) return "<empty>"
        val limit = minOf(bytes.size, maxBytes)
        val sb = StringBuilder()
        var offset = 0
        while (offset < limit) {
            sb.append(String.format("%08X  ", offset))
            val lineLen = minOf(16, limit - offset)
            for (i in 0 until 16) {
                if (i < lineLen) {
                    sb.append(String.format("%02X ", bytes[offset + i].toInt() and 0xFF))
                } else {
                    sb.append("   ")
                }
                if (i == 7) sb.append(' ')
            }
            sb.append(" |")
            for (i in 0 until lineLen) {
                val b = bytes[offset + i].toInt() and 0xFF
                sb.append(if (b in 0x20..0x7E) b.toChar() else '.')
            }
            sb.append("|\n")
            offset += 16
        }
        if (bytes.size > maxBytes) {
            sb.append("... (${bytes.size - maxBytes} more bytes truncated)")
        }
        return sb.toString()
    }
}
