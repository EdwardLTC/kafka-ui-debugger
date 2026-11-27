package dev.edward.kafkadebugger.ui

import com.google.gson.GsonBuilder
import com.google.gson.JsonParser
import com.intellij.ide.setToolTipText
import com.intellij.openapi.application.ApplicationManager
import com.intellij.openapi.project.Project
import com.intellij.openapi.ui.ComboBox
import com.intellij.openapi.util.text.HtmlChunk
import com.intellij.ui.JBColor
import com.intellij.ui.JBSplitter
import com.intellij.ui.components.JBLabel
import com.intellij.ui.components.JBScrollPane
import com.intellij.ui.components.JBTextArea
import com.intellij.ui.components.JBTextField
import com.intellij.ui.table.JBTable
import com.intellij.util.ui.JBUI
import dev.edward.kafkadebugger.kafka.KafkaClientService
import dev.edward.kafkadebugger.model.ClusterConfig
import dev.edward.kafkadebugger.model.MessageHeader
import dev.edward.kafkadebugger.model.TopicInfo
import dev.edward.kafkadebugger.proto.ProtoRegistryService
import java.awt.BorderLayout
import java.awt.Dimension
import java.awt.FlowLayout
import java.awt.Font
import javax.swing.BorderFactory
import javax.swing.JButton
import javax.swing.JPanel
import javax.swing.SwingUtilities
import javax.swing.table.DefaultTableModel

class ProducerPanel(project: Project) : JPanel(BorderLayout()) {

    enum class PayloadFormat {
        UTF8_JSON,
        PROTOBUF_FROM_JSON,
        AVRO_FROM_JSON,
        HEX_BYTES
    }

    private val protoRegistry = ProtoRegistryService.getInstance(project)
    private val gson = GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create()

    private var currentCluster: ClusterConfig? = null

    private val topicCombo = ComboBox<String>().apply { isEditable = true }
    private val partitionField = JBTextField("Auto", 5)
    private val keyField = JBTextField("", 20)
    private val formatCombo = ComboBox(PayloadFormat.entries.toTypedArray())
    private val protoTypeCombo = ComboBox<String>()
    private val confluentSchemaIdField = JBTextField("", 6).apply {
        setToolTipText(HtmlChunk.text("Optional Confluent Schema Registry ID (leave empty for raw protobuf)"))
    }

    private val headersModel = DefaultTableModel(arrayOf("Key", "Value"), 0)
    private val headersTable = JBTable(headersModel)

    private val payloadArea = JBTextArea().apply {
        font = Font(Font.MONOSPACED, Font.PLAIN, 13)
        text = "{\n  \"id\": \"123\",\n  \"status\": \"CREATED\"\n}"
    }
    private val sendButton = JButton("Send Message")
    private val formatJsonButton = JButton("Format JSON")
    private val generateExampleButton = JButton("Generate Example")
    private val statusLabel = JBLabel("Ready to produce.")

    init {
        refreshProtoTypesCombo()
        buildLayout()
        wireListeners()
        updateProtoFieldState()
    }

    private fun buildLayout() {
        // Row 1: Topic / Partition / Key + Send button (always visible, right-aligned)
        val row1 = JPanel(BorderLayout(6, 0)).apply {
            border = JBUI.Borders.empty(3, 0)
            val left = JPanel(FlowLayout(FlowLayout.LEFT, 6, 0)).apply {
                add(JBLabel("Topic:"))
                add(topicCombo.apply { preferredSize = Dimension(200, 26) })
                add(JBLabel("Partition:"))
                add(partitionField)
                add(JBLabel("Key:"))
                add(keyField)
            }
            val right = JPanel(FlowLayout(FlowLayout.RIGHT, 6, 0)).apply {
                add(sendButton.apply {
                    preferredSize = Dimension(130, 28)
                    font = font.deriveFont(Font.BOLD)
                })
            }
            add(left, BorderLayout.CENTER)
            add(right, BorderLayout.EAST)
        }

        // Row 2: Format / Schema / Confluent ID / helper buttons
        val row2 = JPanel(FlowLayout(FlowLayout.LEFT, 6, 3)).apply {
            add(JBLabel("Format:"))
            add(formatCombo)
            add(JBLabel("Proto type:"))
            add(protoTypeCombo.apply { preferredSize = Dimension(220, 26) })
            add(JBLabel("Confluent ID:"))
            add(confluentSchemaIdField)
            add(generateExampleButton)
            add(formatJsonButton)
        }

        val topPanel = JPanel(BorderLayout()).apply {
            border = JBUI.Borders.empty(4)
            add(row1, BorderLayout.NORTH)
            add(row2, BorderLayout.CENTER)
            add(statusLabel.apply { border = JBUI.Borders.empty(2, 6) }, BorderLayout.SOUTH)
        }

        val addHeaderBtn = JButton("+ Header").apply {
            addActionListener { headersModel.addRow(arrayOf("trace-id", "abc-123")) }
        }
        val removeHeaderBtn = JButton("- Remove").apply {
            addActionListener {
                val row = headersTable.selectedRow
                if (row >= 0) headersModel.removeRow(row)
            }
        }
        val headersPanel = JPanel(BorderLayout()).apply {
            border = BorderFactory.createMatteBorder(0, 1, 0, 0, JBColor.border())
            val hdrBar = JPanel(BorderLayout()).apply {
                border = JBUI.Borders.empty(4, 6)
                add(JBLabel("Headers").apply { font = font.deriveFont(Font.BOLD) }, BorderLayout.WEST)
                add(JPanel(FlowLayout(FlowLayout.RIGHT, 4, 0)).apply {
                    add(addHeaderBtn)
                    add(removeHeaderBtn)
                }, BorderLayout.EAST)
            }
            add(hdrBar, BorderLayout.NORTH)
            add(JBScrollPane(headersTable).apply {
                border = BorderFactory.createMatteBorder(1, 0, 0, 0, JBColor.border())
            }, BorderLayout.CENTER)
        }

        val payloadPanel = JPanel(BorderLayout()).apply {
            val payBar = JPanel(BorderLayout()).apply {
                border = JBUI.Borders.empty(4, 6)
                add(JBLabel("Payload").apply { font = font.deriveFont(Font.BOLD) }, BorderLayout.WEST)
            }
            add(payBar, BorderLayout.NORTH)
            add(JBScrollPane(payloadArea).apply {
                border = BorderFactory.createMatteBorder(1, 0, 0, 0, JBColor.border())
            }, BorderLayout.CENTER)
        }

        val splitter = JBSplitter(false, 0.68f).apply {
            firstComponent = payloadPanel
            secondComponent = headersPanel
        }

        add(topPanel, BorderLayout.NORTH)
        add(splitter, BorderLayout.CENTER)
    }

    private fun wireListeners() {
        formatCombo.addActionListener { updateProtoFieldState() }
        protoTypeCombo.addActionListener { autoSuggestFormat() }
        generateExampleButton.addActionListener { generateExample() }
        formatJsonButton.addActionListener {
            try {
                val parsed = JsonParser.parseString(payloadArea.text)
                payloadArea.text = gson.toJson(parsed)
            } catch (e: Exception) {
                statusLabel.foreground = JBColor.RED
                statusLabel.text = "Invalid JSON: ${e.message}"
            }
        }
        sendButton.addActionListener { send() }
    }

    private fun autoSuggestFormat() {
        val selected = protoTypeCombo.selectedItem?.toString() ?: return
        val curFmt = formatCombo.selectedItem
        if (curFmt == PayloadFormat.UTF8_JSON || curFmt == PayloadFormat.HEX_BYTES) {
            formatCombo.selectedItem = if (protoRegistry.isAvroType(selected)) {
                PayloadFormat.AVRO_FROM_JSON
            } else {
                PayloadFormat.PROTOBUF_FROM_JSON
            }
        }
        updateProtoFieldState()
    }

    private fun generateExample() {
        val msgType = protoTypeCombo.selectedItem?.toString()
        if (msgType.isNullOrBlank()) {
            statusLabel.foreground = JBColor.RED
            statusLabel.text = "Select a Schema Type first to generate an example."
            return
        }
        val result = protoRegistry.generateExample(msgType)
        if (result == null) {
            statusLabel.foreground = JBColor.RED
            statusLabel.text = "No schema loaded for '$msgType'. Check Schemas (Proto/Avro)..."
            return
        }
        val (key, json) = result
        keyField.text = key
        payloadArea.text = json
        statusLabel.foreground = JBColor.foreground()
        statusLabel.text = "Example generated from schema: $msgType"
    }

    private fun updateProtoFieldState() {
        val fmt = formatCombo.selectedItem
        val isSchema = fmt == PayloadFormat.PROTOBUF_FROM_JSON || fmt == PayloadFormat.AVRO_FROM_JSON
        protoTypeCombo.isEnabled = isSchema
        confluentSchemaIdField.isEnabled = isSchema
    }

    fun setClusterAndTopics(cluster: ClusterConfig?, topics: List<TopicInfo>) {
        currentCluster = cluster
        val prev = topicCombo.selectedItem?.toString()
        topicCombo.removeAllItems()
        topics.forEach { topicCombo.addItem(it.name) }
        if (prev != null && topics.any { it.name == prev }) {
            topicCombo.selectedItem = prev
        }
    }

    fun refreshProtoTypesCombo() {
        val prev = protoTypeCombo.selectedItem?.toString()
        protoTypeCombo.removeAllItems()
        protoRegistry.getAvailableMessageTypes().forEach { protoTypeCombo.addItem(it) }
        if (prev != null) protoTypeCombo.selectedItem = prev
    }

    fun preFillFromReplay(
        topic: String,
        key: String?,
        payload: String,
        headers: List<MessageHeader>,
        protoType: String?
    ) {
        topicCombo.selectedItem = topic
        keyField.text = key ?: ""
        payloadArea.text = payload
        headersModel.rowCount = 0
        headers.forEach { h ->
            headersModel.addRow(arrayOf(h.key, h.valueAsUtf8()))
        }
        if (protoType != null) {
            formatCombo.selectedItem = if (protoRegistry.isAvroType(protoType)) {
                PayloadFormat.AVRO_FROM_JSON
            } else {
                PayloadFormat.PROTOBUF_FROM_JSON
            }
            protoTypeCombo.selectedItem = protoType
        } else {
            formatCombo.selectedItem = PayloadFormat.UTF8_JSON
        }
        updateProtoFieldState()
        statusLabel.foreground = JBColor.foreground()
        statusLabel.text = "Loaded message from Consumer for replay."
    }

    private fun send() {
        val cluster = currentCluster
        if (cluster == null) {
            statusLabel.foreground = JBColor.RED
            statusLabel.text = "No cluster selected."
            return
        }
        val topic = topicCombo.selectedItem?.toString()?.trim()
        if (topic.isNullOrBlank()) {
            statusLabel.foreground = JBColor.RED
            statusLabel.text = "Topic is required."
            return
        }

        if (headersTable.isEditing) headersTable.cellEditor?.stopCellEditing()

        val partitionText = partitionField.text.trim()
        val partition = if (partitionText.equals("Auto", ignoreCase = true) || partitionText.isEmpty()) {
            null
        } else {
            partitionText.toIntOrNull()
        }

        val keyBytes = keyField.text.let { if (it.isEmpty()) null else it.toByteArray(Charsets.UTF_8) }
        val headers = mutableListOf<MessageHeader>()
        for (i in 0 until headersModel.rowCount) {
            val hk = headersModel.getValueAt(i, 0)?.toString()?.trim() ?: ""
            val hv = headersModel.getValueAt(i, 1)?.toString() ?: ""
            if (hk.isNotEmpty()) {
                headers.add(MessageHeader(hk, hv.toByteArray(Charsets.UTF_8)))
            }
        }

        val valueBytes: ByteArray = try {
            when (formatCombo.selectedItem as PayloadFormat) {
                PayloadFormat.UTF8_JSON -> payloadArea.text.toByteArray(Charsets.UTF_8)
                PayloadFormat.HEX_BYTES -> parseHex(payloadArea.text)
                PayloadFormat.PROTOBUF_FROM_JSON, PayloadFormat.AVRO_FROM_JSON -> {
                    val msgType = protoTypeCombo.selectedItem?.toString()
                        ?: throw IllegalArgumentException("Select a Protobuf / Avro schema type")
                    val schemaId = confluentSchemaIdField.text.trim().toIntOrNull()
                    protoRegistry.encodeJsonToSchema(payloadArea.text, msgType, schemaId)
                }
            }
        } catch (e: Exception) {
            statusLabel.foreground = JBColor.RED
            statusLabel.text = "Encode error: ${e.message}"
            return
        }

        sendButton.isEnabled = false
        statusLabel.foreground = JBColor.foreground()
        statusLabel.text = "Sending..."

        ApplicationManager.getApplication().executeOnPooledThread {
            try {
                val res = KafkaClientService.produceMessage(
                    cluster = cluster,
                    topic = topic,
                    partition = partition,
                    keyBytes = keyBytes,
                    valueBytes = valueBytes,
                    headers = headers
                )
                SwingUtilities.invokeLater {
                    sendButton.isEnabled = true
                    statusLabel.foreground = JBColor.foreground()
                    statusLabel.text =
                        "Sent ${valueBytes.size} B -> ${res.topic}[${res.partition}] @ offset ${res.offset}"
                }
            } catch (e: Exception) {
                SwingUtilities.invokeLater {
                    sendButton.isEnabled = true
                    statusLabel.foreground = JBColor.RED
                    statusLabel.text = "Send failed: ${e.cause?.message ?: e.message}"
                }
            }
        }
    }

    private fun parseHex(hex: String): ByteArray {
        val clean = hex.replace(Regex("\\s+"), "").removePrefix("0x")
        require(clean.length % 2 == 0) { "Hex string must have even length" }
        return ByteArray(clean.length / 2) { i ->
            clean.substring(i * 2, i * 2 + 2).toInt(16).toByte()
        }
    }
}
