package dev.edward.kafkadebugger

import com.google.protobuf.CodedOutputStream
import dev.edward.kafkadebugger.model.ProtoSourceConfig
import dev.edward.kafkadebugger.model.ProtoSourceType
import dev.edward.kafkadebugger.model.TopicProtoMapping
import dev.edward.kafkadebugger.proto.ConfluentWireFormat
import dev.edward.kafkadebugger.proto.ProtoRegistryService
import dev.edward.kafkadebugger.proto.RawProtobufDecoder
import dev.edward.kafkadebugger.state.KafkaDebuggerSettingsState
import org.junit.Assert.*
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.io.ByteArrayOutputStream

class ProtoDebuggerTest {

    @Rule
    @JvmField
    val tempFolder = TemporaryFolder()

    @Test
    fun `load proto directory, encode JSON to protobuf, and decode back with tree`() {
        val protoDir = tempFolder.newFolder("protos")
        val protoFile = java.io.File(protoDir, "order.proto")
        protoFile.writeText(
            """
            syntax = "proto3";
            package demo.orders;

            message Item {
              string sku = 1;
              int32 qty = 2;
            }

            message OrderCreated {
              string order_id = 1;
              int64 amount_cents = 2;
              bool priority = 3;
              repeated Item items = 4;
            }
            """.trimIndent()
        )

        val registry = ProtoRegistryService()
        val errors = registry.reload(
            listOf(ProtoSourceConfig(path = protoDir.absolutePath, type = ProtoSourceType.PROTO_DIR))
        )
        assertTrue("Expected no load errors, got: $errors", errors.isEmpty())

        val types = registry.getAvailableMessageTypes()
        assertTrue(types.contains("demo.orders.OrderCreated"))
        assertTrue(types.contains("demo.orders.Item"))

        val inputJson = """
            {
              "order_id": "ORD-999",
              "amount_cents": 45000,
              "priority": true,
              "items": [
                { "sku": "SKU-1", "qty": 2 }
              ]
            }
        """.trimIndent()

        val encoded = registry.encodeJsonToProtobuf(inputJson, "demo.orders.OrderCreated")
        assertTrue(encoded.isNotEmpty())

        val decoded = registry.decode(encoded, "demo.orders.OrderCreated", stripConfluentHeader = false)
        assertTrue(decoded.prettyJson.contains("ORD-999"))
        assertTrue(decoded.prettyJson.contains("45000"))
        assertTrue(decoded.prettyJson.contains("SKU-1"))
        assertNotNull(decoded.treeRoot)
        assertTrue(decoded.decoderUsed.contains("demo.orders.OrderCreated"))
    }

    @Test
    fun `confluent wire header prepend and strip roundtrip`() {
        val payload = byteArrayOf(0x08, 0x2A, 0x12, 0x03, 'a'.code.toByte(), 'b'.code.toByte(), 'c'.code.toByte())
        val wrapped = ConfluentWireFormat.prependHeader(schemaId = 42, payload = payload)

        val frame = ConfluentWireFormat.tryStripHeader(wrapped)
        assertNotNull(frame)
        assertEquals(42, frame!!.schemaId)
        assertArrayEquals(payload, frame.payload)
    }

    @Test
    fun `raw schemaless protobuf decoder decodes unknown wire bytes`() {
        val baos = ByteArrayOutputStream()
        val out = CodedOutputStream.newInstance(baos)
        out.writeInt64(1, 123456L)
        out.writeString(2, "kafka-debug")
        out.flush()

        val result = RawProtobufDecoder.tryDecode(baos.toByteArray())
        assertNotNull(result)
        assertTrue(result!!.prettyJson.contains("123456"))
        assertTrue(result.prettyJson.contains("kafka-debug"))
        assertEquals(2, result.treeRoot.children.size)
    }

    @Test
    fun `topic pattern matching supports exact and wildcard globs`() {
        val state = KafkaDebuggerSettingsState()
        state.state.topicProtoMappings.add(
            TopicProtoMapping("orders.*.events", "demo.orders.OrderCreated", stripConfluentHeader = true)
        )
        assertNotNull(state.findMappingForTopic("orders.prod.events"))
        assertNull(state.findMappingForTopic("payments.prod.events"))
    }

    @Test
    fun `load avsc file, encode JSON to avro binary with confluent header, and decode back with tree`() {
        val avscFile = tempFolder.newFile("user_event.avsc")
        avscFile.writeText(
            """
            {
              "type": "record",
              "name": "UserSignedUp",
              "namespace": "demo.users",
              "fields": [
                {"name": "userId", "type": "string"},
                {"name": "nickname", "type": ["null", "string"], "default": null},
                {"name": "age", "type": "int"},
                {"name": "active", "type": "boolean"}
              ]
            }
            """.trimIndent()
        )

        val registry = ProtoRegistryService()
        val errors = registry.reload(
            listOf(ProtoSourceConfig(path = avscFile.absolutePath, type = ProtoSourceType.AVRO_FILE))
        )
        assertTrue("Expected no load errors, got: $errors", errors.isEmpty())
        assertTrue(registry.isAvroType("demo.users.UserSignedUp"))

        val inputJson = """{"userId": "u-101", "nickname": "edward", "age": 28, "active": true}"""
        val encoded = registry.encodeJsonToSchema(inputJson, "demo.users.UserSignedUp", confluentSchemaId = 77)
        assertEquals(0x00.toByte(), encoded[0])

        val decoded = registry.decode(encoded, "demo.users.UserSignedUp", stripConfluentHeader = true)
        assertTrue(decoded.prettyJson.contains("u-101"))
        assertTrue(decoded.prettyJson.contains("edward"))
        assertTrue(decoded.prettyJson.contains("28"))
        assertEquals(77, decoded.confluentSchemaId)
        assertNotNull(decoded.treeRoot)
        assertTrue(decoded.decoderUsed.contains("Avro (demo.users.UserSignedUp)"))
    }
}
