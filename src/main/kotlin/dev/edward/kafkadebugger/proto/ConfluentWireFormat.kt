package dev.edward.kafkadebugger.proto

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

data class ConfluentFrame(
    val schemaId: Int,
    val messageIndexes: List<Int>,
    val payload: ByteArray
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as ConfluentFrame

        if (schemaId != other.schemaId) return false
        if (messageIndexes != other.messageIndexes) return false
        if (!payload.contentEquals(other.payload)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = schemaId
        result = 31 * result + messageIndexes.hashCode()
        result = 31 * result + payload.contentHashCode()
        return result
    }
}

object ConfluentWireFormat {
    private const val MAGIC_BYTE: Byte = 0x00

    fun tryStripHeader(bytes: ByteArray): ConfluentFrame? {
        if (bytes.size < 6 || bytes[0] != MAGIC_BYTE) return null
        val schemaId = ByteBuffer.wrap(bytes, 1, 4).int
        if (schemaId <= 0) return null

        var pos = 5
        val (count, nextPos) = readVarint(bytes, pos) ?: return null
        pos = nextPos
        val indexes = mutableListOf<Int>()
        if (count == 0) {
            indexes.add(0)
        } else {
            if (count !in 0..64) return null
            repeat(count) {
                val (idx, afterIdx) = readVarint(bytes, pos) ?: return null
                indexes.add(idx)
                pos = afterIdx
            }
        }
        if (pos > bytes.size) return null
        return ConfluentFrame(
            schemaId = schemaId,
            messageIndexes = indexes,
            payload = bytes.copyOfRange(pos, bytes.size)
        )
    }

    fun prependHeader(schemaId: Int, payload: ByteArray, messageIndex: Int = 0): ByteArray {
        val out = ByteArrayOutputStream(payload.size + 8)
        out.write(MAGIC_BYTE.toInt())
        val buf = ByteBuffer.allocate(4).putInt(schemaId).array()
        out.write(buf)
        if (messageIndex == 0) {
            out.write(0x00)
        } else {
            writeVarint(out, 1)
            writeVarint(out, messageIndex)
        }
        out.write(payload)
        return out.toByteArray()
    }

    /**
     * Confluent Avro wire format: 1 magic byte (0x00) + 4-byte big-endian schemaId + Avro binary payload.
     */
    fun tryStripAvroHeader(bytes: ByteArray): Pair<Int, ByteArray>? {
        if (bytes.size < 5 || bytes[0] != MAGIC_BYTE) return null
        val schemaId = ByteBuffer.wrap(bytes, 1, 4).int
        if (schemaId <= 0) return null
        return schemaId to bytes.copyOfRange(5, bytes.size)
    }

    fun prependAvroHeader(schemaId: Int, payload: ByteArray): ByteArray {
        val out = ByteArrayOutputStream(payload.size + 5)
        out.write(MAGIC_BYTE.toInt())
        out.write(ByteBuffer.allocate(4).putInt(schemaId).array())
        out.write(payload)
        return out.toByteArray()
    }

    private fun readVarint(bytes: ByteArray, start: Int): Pair<Int, Int>? {
        var pos = start
        var result = 0
        var shift = 0
        while (pos < bytes.size && shift < 35) {
            val b = bytes[pos++].toInt() and 0xFF
            result = result or ((b and 0x7F) shl shift)
            if ((b and 0x80) == 0) {
                // ZigZag decode for Confluent signed varint
                val decoded = (result ushr 1) xor -(result and 1)
                return decoded to pos
            }
            shift += 7
        }
        return null
    }

    private fun writeVarint(out: ByteArrayOutputStream, value: Int) {
        var v = (value shl 1) xor (value shr 31)
        while ((v and 0x7F.inv()) != 0) {
            out.write((v and 0x7F) or 0x80)
            v = v ushr 7
        }
        out.write(v and 0x7F)
    }
}
