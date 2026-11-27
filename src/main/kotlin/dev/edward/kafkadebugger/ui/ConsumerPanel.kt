package dev.edward.kafkadebugger.ui

import com.intellij.ide.setToolTipText
import com.intellij.openapi.application.ApplicationManager
import com.intellij.openapi.ide.CopyPasteManager
import com.intellij.openapi.project.Project
import com.intellij.openapi.ui.ComboBox
import com.intellij.openapi.util.text.HtmlChunk
import com.intellij.ui.JBSplitter
import com.intellij.ui.components.*
import com.intellij.ui.table.JBTable
import com.intellij.ui.treeStructure.Tree
import com.intellij.util.ui.JBUI
import dev.edward.kafkadebugger.kafka.KafkaClientService
import dev.edward.kafkadebugger.model.*
import dev.edward.kafkadebugger.proto.DecodedPayload
import dev.edward.kafkadebugger.proto.ProtoRegistryService
import dev.edward.kafkadebugger.proto.ProtoTreeNode
import dev.edward.kafkadebugger.state.KafkaDebuggerSettingsState
import java.awt.BorderLayout
import java.awt.FlowLayout
import java.awt.Font
import java.awt.datatransfer.StringSelection
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.concurrent.atomic.AtomicBoolean
import javax.swing.*
import javax.swing.event.DocumentEvent
import javax.swing.event.DocumentListener
import javax.swing.table.DefaultTableModel
import javax.swing.tree.DefaultMutableTreeNode
import javax.swing.tree.DefaultTreeModel

class ConsumerPanel(
    project: Project,
    private val onReplayRequested: (topic: String, key: String?, payload: String, headers: List<MessageHeader>, protoType: String?) -> Unit
) : JPanel(BorderLayout()) {

    private val settings = KafkaDebuggerSettingsState.getInstance(project)
    private val protoRegistry = ProtoRegistryService.getInstance(project)

    private var currentCluster: ClusterConfig? = null
    private var currentTopicInfo: TopicInfo? = null
    private val stopSignal = AtomicBoolean(true)

    private val allRecords = mutableListOf<KafkaMessageRecord>()
    private val filteredRecords = mutableListOf<KafkaMessageRecord>()
    private val decodedCache = mutableMapOf<Int, DecodedPayload>()

    private val topicCombo = ComboBox<String>().apply { isEditable = true }
    private val partitionCombo = ComboBox(arrayOf("All"))
    private val modeCombo = ComboBox(ConsumeMode.entries.toTypedArray())
    private val paramField = JBTextField("100", 7)
    private val liveTailCheck = JBCheckBox("Live tail", false)
    private val consumeButton = JButton("Consume")
    private val clearButton = JButton("Clear")

    private val protoTypeCombo = ComboBox<String>()
    private val stripConfluentCheck = JBCheckBox("Confluent", false).apply {
        HtmlChunk.text("Strip Confluent Schema Registry wire header")
    }
    private val filterField = JBTextField(18).apply {
        emptyText.text = "Filter key, value, offset..."
    }

    private val statusLabel = JBLabel("Ready.").apply {
        foreground = com.intellij.ui.JBColor.GRAY
        font = font.deriveFont(11f)
    }
    private val decoderBadgeLabel = JBLabel("Decoder: -").apply {
        foreground = com.intellij.ui.JBColor.GRAY
        font = font.deriveFont(11f)
    }

    private val timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
        .withZone(ZoneId.systemDefault())

    private val messageTableModel = object : DefaultTableModel(
        arrayOf("Offset", "Part", "Timestamp", "Key", "Value Preview", "Size", "Headers"), 0
    ) {
        override fun isCellEditable(row: Int, column: Int) = false
    }
    private val messageTable = JBTable(messageTableModel).apply {
        setSelectionMode(ListSelectionModel.SINGLE_SELECTION)
        autoResizeMode = JTable.AUTO_RESIZE_SUBSEQUENT_COLUMNS
        columnModel.getColumn(0).apply { preferredWidth = 65; maxWidth = 100 }
        columnModel.getColumn(1).apply { preferredWidth = 45; maxWidth = 70 }
        columnModel.getColumn(2).apply { preferredWidth = 175; minWidth = 150 }
        columnModel.getColumn(3).apply { preferredWidth = 130; minWidth = 80 }
        columnModel.getColumn(4).apply { preferredWidth = 480; minWidth = 200 }
        columnModel.getColumn(5).apply { preferredWidth = 70; maxWidth = 100 }
        columnModel.getColumn(6).apply { preferredWidth = 65; maxWidth = 90 }
    }

    private val jsonArea = JBTextArea().apply {
        isEditable = false
        font = Font(Font.MONOSPACED, Font.PLAIN, 12)
        lineWrap = false
    }
    private val rawUtf8Area = JBTextArea().apply {
        isEditable = false
        font = Font(Font.MONOSPACED, Font.PLAIN, 12)
    }
    private val hexArea = JBTextArea().apply {
        isEditable = false
        font = Font(Font.MONOSPACED, Font.PLAIN, 12)
    }
    private val keyArea = JBTextArea(3, 40).apply {
        isEditable = false
        font = Font(Font.MONOSPACED, Font.PLAIN, 12)
    }
    private val headersTableModel = object : DefaultTableModel(arrayOf("Header Key", "Value (UTF-8)", "Hex"), 0) {
        override fun isCellEditable(row: Int, column: Int) = false
    }
    private val headersTable = JBTable(headersTableModel)

    private val protoTreeRoot = DefaultMutableTreeNode("Select a message to inspect")
    private val protoTree = Tree(DefaultTreeModel(protoTreeRoot))

    private var selectedRecord: KafkaMessageRecord? = null
    private var selectedDecoded: DecodedPayload? = null

    init {
        refreshProtoTypesCombo()
        buildLayout()
        wireListeners()
    }

    private fun buildLayout() {
        val topLeft = JPanel(FlowLayout(FlowLayout.LEFT, 5, 2)).apply {
            add(topicCombo.apply {
                preferredSize = java.awt.Dimension(210, 26); setToolTipText(HtmlChunk.text("Topic"))
            })
            add(JBLabel("Part:").apply { foreground = com.intellij.ui.JBColor.GRAY })
            add(partitionCombo)
            add(modeCombo)
            add(paramField)
            add(liveTailCheck)
        }
        val topRight = JPanel(FlowLayout(FlowLayout.RIGHT, 5, 2)).apply {
            add(consumeButton.apply { font = font.deriveFont(Font.BOLD) })
            add(clearButton)
        }
        val topRow = JPanel(BorderLayout()).apply {
            add(topLeft, BorderLayout.CENTER)
            add(topRight, BorderLayout.EAST)
        }

        val protoLeft = JPanel(FlowLayout(FlowLayout.LEFT, 5, 2)).apply {
            add(filterField)
            add(JBLabel("Schema:").apply { foreground = com.intellij.ui.JBColor.GRAY })
            add(protoTypeCombo.apply { preferredSize = java.awt.Dimension(230, 26) })
            add(stripConfluentCheck)
        }
        val protoRow = JPanel(BorderLayout()).apply {
            add(protoLeft, BorderLayout.CENTER)
            add(statusLabel.apply { border = JBUI.Borders.empty(0, 6) }, BorderLayout.EAST)
        }

        val toolbarPanel = JPanel(BorderLayout()).apply {
            border = BorderFactory.createCompoundBorder(
                BorderFactory.createMatteBorder(0, 0, 1, 0, com.intellij.ui.JBColor.border()),
                JBUI.Borders.empty(3, 6)
            )
            add(topRow, BorderLayout.NORTH)
            add(protoRow, BorderLayout.SOUTH)
        }

        val detailTabs = JBTabbedPane().apply {
            val copyBtn = JButton("Copy JSON").apply {
                addActionListener {
                    CopyPasteManager.getInstance().contents = StringSelection(jsonArea.text)
                }
            }
            val replayBtn = JButton("Replay in Producer ->").apply {
                addActionListener { replaySelectedMessage() }
            }
            val jsonBar = JPanel(BorderLayout(8, 0)).apply {
                border = JBUI.Borders.empty(2, 4)
                decoderBadgeLabel.minimumSize = java.awt.Dimension(0, 20)
                add(decoderBadgeLabel, BorderLayout.CENTER)
                add(JPanel(FlowLayout(FlowLayout.RIGHT, 6, 0)).apply {
                    add(copyBtn)
                    add(replayBtn)
                }, BorderLayout.EAST)
            }
            val jsonPanel = JPanel(BorderLayout()).apply {
                add(jsonBar, BorderLayout.NORTH)
                add(JBScrollPane(jsonArea), BorderLayout.CENTER)
            }

            val keyAndHeadersPanel = JPanel(BorderLayout(0, 6)).apply {
                border = JBUI.Borders.empty(4)
                add(JPanel(BorderLayout()).apply {
                    border = BorderFactory.createTitledBorder("Message Key")
                    add(JBScrollPane(keyArea), BorderLayout.CENTER)
                }, BorderLayout.NORTH)
                add(JPanel(BorderLayout()).apply {
                    border = BorderFactory.createTitledBorder("Headers")
                    add(JBScrollPane(headersTable), BorderLayout.CENTER)
                }, BorderLayout.CENTER)
            }

            addTab("Decoded", jsonPanel)
            addTab("Tree", JBScrollPane(protoTree))
            addTab("Headers & Key", keyAndHeadersPanel)
            addTab("Raw", JBScrollPane(rawUtf8Area))
            addTab("Hex", JBScrollPane(hexArea))
        }

        val tableScroll = JBScrollPane(messageTable).apply {
            minimumSize = java.awt.Dimension(0, 80)
            preferredSize = java.awt.Dimension(0, 200)
        }
        detailTabs.apply {
            minimumSize = java.awt.Dimension(0, 80)
            preferredSize = java.awt.Dimension(0, 200)
        }

        val splitter = JBSplitter(true, 0.48f).apply {
            setHonorComponentsMinimumSize(false)
            firstComponent = tableScroll
            secondComponent = detailTabs
        }

        add(toolbarPanel, BorderLayout.NORTH)
        add(splitter, BorderLayout.CENTER)
    }

    private fun wireListeners() {
        consumeButton.addActionListener {
            if (stopSignal.get()) startConsume() else stopConsume()
        }
        clearButton.addActionListener {
            allRecords.clear()
            filteredRecords.clear()
            decodedCache.clear()
            messageTableModel.rowCount = 0
            clearDetail()
            statusLabel.text = "Cleared."
        }
        modeCombo.addActionListener {
            when (modeCombo.selectedItem as ConsumeMode) {
                ConsumeMode.LAST_N -> {
                    paramField.isEnabled = true
                    paramField.text = "100"
                    paramField.setToolTipText(HtmlChunk.text("Number of latest messages"))
                }

                ConsumeMode.FROM_OFFSET -> {
                    paramField.isEnabled = true
                    paramField.text = "0"
                    paramField.setToolTipText(HtmlChunk.text("Start offset"))
                }

                ConsumeMode.FROM_TIMESTAMP -> {
                    paramField.isEnabled = true
                    paramField.text = (System.currentTimeMillis() - 3600_000L).toString()
                    paramField.setToolTipText(HtmlChunk.text("Epoch milliseconds"))
                }

                else -> paramField.isEnabled = false
            }
        }
        topicCombo.addActionListener {
            applyTopicMapping()
        }
        protoTypeCombo.addActionListener {
            redecodeVisibleMessages()
        }
        stripConfluentCheck.addActionListener {
            redecodeVisibleMessages()
        }
        filterField.document.addDocumentListener(object : DocumentListener {
            override fun insertUpdate(e: DocumentEvent?) = applyFilter()
            override fun removeUpdate(e: DocumentEvent?) = applyFilter()
            override fun changedUpdate(e: DocumentEvent?) = applyFilter()
        })
        messageTable.selectionModel.addListSelectionListener { e ->
            if (!e.valueIsAdjusting) {
                val row = messageTable.selectedRow
                if (row in filteredRecords.indices) {
                    showRecordDetail(filteredRecords[row])
                }
            }
        }
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

    fun selectTopic(topicInfo: TopicInfo) {
        currentTopicInfo = topicInfo
        topicCombo.selectedItem = topicInfo.name
        partitionCombo.removeAllItems()
        partitionCombo.addItem("All")
        topicInfo.partitions.forEach { partitionCombo.addItem(it.partition.toString()) }
        applyTopicMapping()
    }

    fun refreshProtoTypesCombo() {
        val prev = protoTypeCombo.selectedItem?.toString()
        protoTypeCombo.removeAllItems()
        protoTypeCombo.addItem("<Auto / Schemaless>")
        protoRegistry.getAvailableMessageTypes().forEach { protoTypeCombo.addItem(it) }
        if (prev != null) {
            protoTypeCombo.selectedItem = prev
        }
    }

    private fun applyTopicMapping() {
        val topic = topicCombo.selectedItem?.toString()?.trim() ?: return
        val mapping = settings.findMappingForTopic(topic)
        if (mapping != null) {
            protoTypeCombo.selectedItem = mapping.messageFullName
            stripConfluentCheck.isSelected = mapping.stripConfluentHeader
        }
    }

    private fun selectedProtoType(): String? {
        val sel = protoTypeCombo.selectedItem?.toString()
        return if (sel.isNullOrBlank() || sel == "<Auto / Schemaless>") null else sel
    }

    private fun startConsume() {
        val cluster = currentCluster
        if (cluster == null) {
            statusLabel.text = "No cluster selected."
            return
        }
        val topic = topicCombo.selectedItem?.toString()?.trim()
        if (topic.isNullOrBlank()) {
            statusLabel.text = "Select a topic first."
            return
        }

        val partSel = partitionCombo.selectedItem?.toString()
        val partition = if (partSel == null || partSel == "All") null else partSel.toIntOrNull()
        val mode = modeCombo.selectedItem as ConsumeMode
        val paramVal = paramField.text.trim().toLongOrNull() ?: 100L

        val request = ConsumeRequest(
            cluster = cluster,
            topic = topic,
            partition = partition,
            mode = mode,
            limit = if (mode == ConsumeMode.LAST_N) paramVal.toInt().coerceIn(1, 5000) else 500,
            startOffset = if (mode == ConsumeMode.FROM_OFFSET) paramVal else 0L,
            startTimestamp = if (mode == ConsumeMode.FROM_TIMESTAMP) paramVal else System.currentTimeMillis() - 3600_000L,
            liveTail = liveTailCheck.isSelected
        )

        allRecords.clear()
        filteredRecords.clear()
        decodedCache.clear()
        messageTableModel.rowCount = 0
        clearDetail()

        stopSignal.set(false)
        consumeButton.text = "Stop"
        statusLabel.text = if (request.liveTail) "Live tailing $topic..." else "Consuming $topic..."

        ApplicationManager.getApplication().executeOnPooledThread {
            try {
                KafkaClientService.consumeMessages(request, stopSignal) { batch ->
                    SwingUtilities.invokeLater {
                        appendRecords(batch)
                    }
                }
                SwingUtilities.invokeLater {
                    stopSignal.set(true)
                    consumeButton.text = "Consume"
                    statusLabel.text = "Done (${allRecords.size} messages)."
                }
            } catch (e: Exception) {
                SwingUtilities.invokeLater {
                    stopSignal.set(true)
                    consumeButton.text = "Consume"
                    statusLabel.text = "Error: ${e.cause?.message ?: e.message}"
                }
            }
        }
    }

    fun stopConsume() {
        stopSignal.set(true)
        consumeButton.text = "Consume"
        statusLabel.text = "Stopped (${allRecords.size} messages)."
    }

    private fun appendRecords(batch: List<KafkaMessageRecord>) {
        val protoType = selectedProtoType()
        val strip = stripConfluentCheck.isSelected
        for (rec in batch) {
            val idx = allRecords.size
            allRecords.add(rec)
            decodedCache[idx] = protoRegistry.decode(rec.valueBytes, protoType, strip)
        }
        applyFilter()
        statusLabel.text = "Loaded ${allRecords.size} messages..."
    }

    private fun redecodeVisibleMessages() {
        val protoType = selectedProtoType()
        val strip = stripConfluentCheck.isSelected
        decodedCache.clear()
        allRecords.forEachIndexed { idx, rec ->
            decodedCache[idx] = protoRegistry.decode(rec.valueBytes, protoType, strip)
        }
        applyFilter()
        selectedRecord?.let { showRecordDetail(it) }
    }

    private fun applyFilter() {
        val query = filterField.text.trim().lowercase()
        filteredRecords.clear()
        messageTableModel.rowCount = 0

        allRecords.forEachIndexed { idx, rec ->
            val decoded = decodedCache[idx]
            val keyStr = rec.keyAsUtf8() ?: ""
            val preview = (decoded?.prettyJson ?: "").replace("\n", " ").replace(Regex("\\s+"), " ")
            val matches = query.isEmpty() ||
                    keyStr.lowercase().contains(query) ||
                    preview.lowercase().contains(query) ||
                    rec.offset.toString().contains(query) ||
                    rec.headers.any {
                        it.key.lowercase().contains(query) || it.valueAsUtf8().lowercase().contains(query)
                    }

            if (matches) {
                filteredRecords.add(rec)
                messageTableModel.addRow(
                    arrayOf(
                        rec.offset,
                        rec.partition,
                        timeFormatter.format(Instant.ofEpochMilli(rec.timestamp)),
                        keyStr.ifEmpty { "<null>" },
                        preview.take(140),
                        "${rec.valueBytes?.size ?: 0} B",
                        rec.headers.size
                    )
                )
            }
        }
    }

    private fun showRecordDetail(rec: KafkaMessageRecord) {
        selectedRecord = rec
        val decoded = protoRegistry.decode(
            rec.valueBytes,
            selectedProtoType(),
            stripConfluentCheck.isSelected
        )
        selectedDecoded = decoded

        val note = if (decoded.errorNote != null) " | ${decoded.errorNote}" else ""
        val badgeText = "Decoder: ${decoded.decoderUsed}$note"
        decoderBadgeLabel.text = badgeText
        decoderBadgeLabel.setToolTipText(HtmlChunk.text(badgeText))

        jsonArea.text = decoded.prettyJson
        jsonArea.caretPosition = 0
        rawUtf8Area.text = decoded.rawUtf8
        rawUtf8Area.caretPosition = 0
        hexArea.text = decoded.hexDump
        hexArea.caretPosition = 0

        keyArea.text = rec.keyAsUtf8() ?: "<null>"
        headersTableModel.rowCount = 0
        rec.headers.forEach { h ->
            val hex = h.value?.joinToString(" ") { String.format("%02X", it.toInt() and 0xFF) } ?: "<null>"
            headersTableModel.addRow(arrayOf(h.key, h.valueAsUtf8(), hex))
        }

        val rootNode = if (decoded.treeRoot != null) {
            toSwingTreeNode(decoded.treeRoot)
        } else {
            DefaultMutableTreeNode("No structured Protobuf tree (${decoded.decoderUsed})")
        }
        protoTree.model = DefaultTreeModel(rootNode)
        for (i in 0 until minOf(protoTree.rowCount, 12)) {
            protoTree.expandRow(i)
        }
    }

    private fun toSwingTreeNode(node: ProtoTreeNode): DefaultMutableTreeNode {
        val swingNode = DefaultMutableTreeNode(node.toString())
        node.children.forEach { child ->
            swingNode.add(toSwingTreeNode(child))
        }
        return swingNode
    }

    private fun clearDetail() {
        selectedRecord = null
        selectedDecoded = null
        decoderBadgeLabel.text = "Decoder: -"
        jsonArea.text = ""
        rawUtf8Area.text = ""
        hexArea.text = ""
        keyArea.text = ""
        headersTableModel.rowCount = 0
        protoTree.model = DefaultTreeModel(DefaultMutableTreeNode("Select a message to inspect"))
    }

    private fun replaySelectedMessage() {
        val rec = selectedRecord ?: return
        val decoded = selectedDecoded
        val payloadText = decoded?.prettyJson ?: rec.valueBytes?.toString(Charsets.UTF_8) ?: ""
        onReplayRequested(
            rec.topic,
            rec.keyAsUtf8(),
            payloadText,
            rec.headers,
            selectedProtoType()
        )
    }
}
