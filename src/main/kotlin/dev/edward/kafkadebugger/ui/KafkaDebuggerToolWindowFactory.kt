package dev.edward.kafkadebugger.ui

import com.intellij.icons.AllIcons
import com.intellij.openapi.application.ApplicationManager
import com.intellij.openapi.project.DumbAware
import com.intellij.openapi.project.Project
import com.intellij.openapi.ui.ComboBox
import com.intellij.openapi.ui.Messages
import com.intellij.openapi.wm.ToolWindow
import com.intellij.openapi.wm.ToolWindowFactory
import com.intellij.ui.ColoredListCellRenderer
import com.intellij.ui.JBColor
import com.intellij.ui.JBSplitter
import com.intellij.ui.SimpleTextAttributes
import com.intellij.ui.components.*
import com.intellij.ui.content.ContentFactory
import com.intellij.util.ui.JBUI
import dev.edward.kafkadebugger.kafka.KafkaClientService
import dev.edward.kafkadebugger.model.ClusterConfig
import dev.edward.kafkadebugger.model.TopicInfo
import dev.edward.kafkadebugger.proto.ProtoRegistryService
import dev.edward.kafkadebugger.state.KafkaDebuggerSettingsState
import java.awt.BorderLayout
import java.awt.Dimension
import java.awt.FlowLayout
import java.awt.event.MouseAdapter
import java.awt.event.MouseEvent
import javax.swing.*
import javax.swing.event.DocumentEvent
import javax.swing.event.DocumentListener

class KafkaDebuggerToolWindowFactory : ToolWindowFactory, DumbAware {

    override fun shouldBeAvailable(project: Project): Boolean = true

    override fun createToolWindowContent(project: Project, toolWindow: ToolWindow) {
        val panel = KafkaDebuggerMainPanel(project)
        val content = ContentFactory.getInstance().createContent(panel, "", false)
        toolWindow.contentManager.addContent(content)
    }
}

class KafkaDebuggerMainPanel(private val project: Project) : JPanel(BorderLayout()) {

    private val settings = KafkaDebuggerSettingsState.getInstance(project)
    private val protoRegistry = ProtoRegistryService.getInstance(project)

    private var allTopics: List<TopicInfo> = emptyList()
    private val filteredTopics = mutableListOf<TopicInfo>()

    private val clusterCombo = ComboBox<ClusterConfig>()
    private val refreshBtn = iconButton(AllIcons.Actions.Refresh, "Connect / Refresh")
    private val addClusterBtn = iconButton(AllIcons.General.Add, "Add Cluster")
    private val editClusterBtn = iconButton(AllIcons.Actions.Edit, "Edit Selected Cluster")
    private val delClusterBtn = iconButton(AllIcons.General.Remove, "Remove Selected Cluster")
    private val protoSettingsBtn = JButton("Schemas (Proto/Avro)...", AllIcons.FileTypes.JsonSchema)

    private val topicFilterField = JBTextField().apply {
        emptyText.text = "Filter topics..."
    }
    private val internalCheck = JBCheckBox("Internal", settings.state.showInternalTopics)
    private val topicListModel = DefaultListModel<TopicInfo>()
    private val topicList = JBList(topicListModel).apply {
        selectionMode = ListSelectionModel.SINGLE_SELECTION
        cellRenderer = object : ColoredListCellRenderer<TopicInfo>() {
            override fun customizeCellRenderer(
                list: JList<out TopicInfo>,
                value: TopicInfo?,
                index: Int,
                selected: Boolean,
                hasFocus: Boolean
            ) {
                if (value == null) return
                icon = AllIcons.Nodes.PpWeb
                append(value.name, SimpleTextAttributes.REGULAR_ATTRIBUTES)
                append("  ${value.partitionCount}p · ${value.totalMessages}", SimpleTextAttributes.GRAYED_ATTRIBUTES)
            }
        }
    }
    private val explorerStatusLabel = JBLabel("Not connected.").apply {
        foreground = JBColor.GRAY
        font = font.deriveFont(11f)
    }

    private val rightTabs = JBTabbedPane()
    private var consumerPanel: ConsumerPanel
    private var producerPanel: ProducerPanel
    private val topicInspectorPanel = TopicInspectorPanel()
    private val consumerGroupsPanel = ConsumerGroupsPanel()

    init {
        protoRegistry.reload(settings.state.protoSources)

        producerPanel = ProducerPanel(project)
        consumerPanel = ConsumerPanel(project) { topic, key, payload, headers, protoType ->
            producerPanel.preFillFromReplay(topic, key, payload, headers, protoType)
            rightTabs.selectedIndex = 1
        }

        rightTabs.addTab("Message Debugger", consumerPanel)
        rightTabs.addTab("Producer", producerPanel)
        rightTabs.addTab("Topic Inspector", topicInspectorPanel)
        rightTabs.addTab("Consumer Groups", consumerGroupsPanel)

        val leftExplorer = buildLeftExplorer().apply {
            minimumSize = Dimension(140, 0)
            preferredSize = Dimension(220, 0)
        }
        rightTabs.minimumSize = Dimension(250, 0)
        rightTabs.preferredSize = Dimension(600, 0)

        val splitter = JBSplitter(false, 0.24f).apply {
            setHonorComponentsMinimumSize(false)
            firstComponent = leftExplorer
            secondComponent = rightTabs
        }

        add(buildTopHeader(), BorderLayout.NORTH)
        add(splitter, BorderLayout.CENTER)

        reloadClustersCombo()
        wireListeners()
    }

    private fun iconButton(icon: Icon, tooltip: String): JButton = JButton(icon).apply {
        toolTipText = tooltip
        preferredSize = Dimension(28, 28)
        isFocusable = false
        isContentAreaFilled = false
        border = JBUI.Borders.empty(2)
    }

    private fun buildTopHeader(): JComponent {
        val left = JPanel(FlowLayout(FlowLayout.LEFT, 4, 2)).apply {
            add(JBLabel("Cluster:").apply { foreground = JBColor.GRAY })
            add(clusterCombo.apply { preferredSize = Dimension(230, 26) })
            add(addClusterBtn)
            add(editClusterBtn)
            add(delClusterBtn)
            add(refreshBtn)
        }
        val right = JPanel(FlowLayout(FlowLayout.RIGHT, 4, 2)).apply {
            add(protoSettingsBtn)
        }
        return JPanel(BorderLayout()).apply {
            border = BorderFactory.createCompoundBorder(
                BorderFactory.createMatteBorder(0, 0, 1, 0, JBColor.border()),
                JBUI.Borders.empty(3, 6)
            )
            add(left, BorderLayout.WEST)
            add(right, BorderLayout.EAST)
        }
    }

    private fun buildLeftExplorer(): JComponent {
        val addTopicBtn = iconButton(AllIcons.General.Add, "Create Topic").apply {
            addActionListener { createTopicDialog() }
        }
        val delTopicBtn = iconButton(AllIcons.Actions.GC, "Delete Selected Topic").apply {
            addActionListener { deleteSelectedTopic() }
        }

        val headerBar = JPanel(BorderLayout()).apply {
            border = JBUI.Borders.empty(4, 6, 2, 4)
            add(JBLabel("Topics").apply { font = font.deriveFont(java.awt.Font.BOLD) }, BorderLayout.WEST)
            add(JPanel(FlowLayout(FlowLayout.RIGHT, 2, 0)).apply {
                add(internalCheck)
                add(addTopicBtn)
                add(delTopicBtn)
            }, BorderLayout.EAST)
        }

        val searchBar = JPanel(BorderLayout()).apply {
            border = JBUI.Borders.empty(2, 6, 4, 6)
            add(topicFilterField, BorderLayout.CENTER)
        }

        val topBox = JPanel(BorderLayout()).apply {
            add(headerBar, BorderLayout.NORTH)
            add(searchBar, BorderLayout.SOUTH)
        }

        val scroll = JBScrollPane(topicList).apply {
            border = BorderFactory.createMatteBorder(1, 0, 1, 0, JBColor.border())
        }

        return JPanel(BorderLayout()).apply {
            border = BorderFactory.createMatteBorder(0, 0, 0, 1, JBColor.border())
            add(topBox, BorderLayout.NORTH)
            add(scroll, BorderLayout.CENTER)
            add(explorerStatusLabel.apply { border = JBUI.Borders.empty(3, 6) }, BorderLayout.SOUTH)
        }
    }

    private fun wireListeners() {
        refreshBtn.addActionListener { refreshCluster() }
        addClusterBtn.addActionListener {
            val dlg = ClusterDialog(project)
            if (dlg.showAndGet()) {
                val cfg = dlg.buildClusterConfig()
                settings.state.clusters.add(cfg)
                reloadClustersCombo(selectId = cfg.id)
                refreshCluster()
            }
        }
        editClusterBtn.addActionListener {
            val current = selectedCluster() ?: return@addActionListener
            val dlg = ClusterDialog(project, current)
            if (dlg.showAndGet()) {
                val updated = dlg.buildClusterConfig()
                val idx = settings.state.clusters.indexOfFirst { it.id == current.id }
                if (idx >= 0) {
                    settings.state.clusters[idx] = updated
                    reloadClustersCombo(selectId = updated.id)
                }
            }
        }
        delClusterBtn.addActionListener {
            val current = selectedCluster() ?: return@addActionListener
            val confirm = Messages.showYesNoDialog(
                project,
                "Remove cluster '${current.name}'?",
                "Remove Cluster",
                Messages.getQuestionIcon()
            )
            if (confirm == Messages.YES) {
                settings.state.clusters.removeIf { it.id == current.id }
                reloadClustersCombo()
            }
        }
        protoSettingsBtn.addActionListener {
            val dlg = ProtoSettingsDialog(project)
            if (dlg.showAndGet()) {
                consumerPanel.refreshProtoTypesCombo()
                producerPanel.refreshProtoTypesCombo()
            }
        }
        internalCheck.addActionListener {
            settings.state.showInternalTopics = internalCheck.isSelected
            refreshCluster()
        }
        topicFilterField.document.addDocumentListener(object : DocumentListener {
            override fun insertUpdate(e: DocumentEvent?) = applyTopicFilter()
            override fun removeUpdate(e: DocumentEvent?) = applyTopicFilter()
            override fun changedUpdate(e: DocumentEvent?) = applyTopicFilter()
        })
        topicList.addMouseListener(object : MouseAdapter() {
            override fun mouseClicked(e: MouseEvent) {
                val idx = topicList.selectedIndex
                if (idx in filteredTopics.indices) {
                    val topic = filteredTopics[idx]
                    consumerPanel.selectTopic(topic)
                    selectedCluster()?.let { topicInspectorPanel.loadTopic(it, topic.name) }
                    if (e.clickCount >= 2) {
                        rightTabs.selectedIndex = 0
                    }
                }
            }
        })
    }

    private fun reloadClustersCombo(selectId: String? = null) {
        val prevId = selectId ?: selectedCluster()?.id
        clusterCombo.removeAllItems()
        settings.state.clusters.forEach { clusterCombo.addItem(it) }
        if (prevId != null) {
            val match = settings.state.clusters.firstOrNull { it.id == prevId }
            if (match != null) clusterCombo.selectedItem = match
        }
    }

    private fun selectedCluster(): ClusterConfig? = clusterCombo.selectedItem as? ClusterConfig

    private fun refreshCluster() {
        val cluster = selectedCluster() ?: return
        refreshBtn.isEnabled = false
        explorerStatusLabel.text = "Connecting to ${cluster.bootstrapServers}..."

        ApplicationManager.getApplication().executeOnPooledThread {
            try {
                val topics = KafkaClientService.listTopics(cluster, internalCheck.isSelected)
                SwingUtilities.invokeLater {
                    refreshBtn.isEnabled = true
                    allTopics = topics
                    applyTopicFilter()
                    consumerPanel.setClusterAndTopics(cluster, topics)
                    producerPanel.setClusterAndTopics(cluster, topics)
                    consumerGroupsPanel.setCluster(cluster)
                    explorerStatusLabel.text = "${topics.size} topics loaded."
                }
            } catch (e: Exception) {
                SwingUtilities.invokeLater {
                    refreshBtn.isEnabled = true
                    explorerStatusLabel.text = "Error: ${e.cause?.message ?: e.message}"
                }
            }
        }
    }

    private fun applyTopicFilter() {
        val q = topicFilterField.text.trim().lowercase()
        filteredTopics.clear()
        topicListModel.clear()
        allTopics.filter { q.isEmpty() || it.name.lowercase().contains(q) }.forEach { t ->
            filteredTopics.add(t)
            topicListModel.addElement(t)
        }
    }

    private fun createTopicDialog() {
        val cluster = selectedCluster() ?: return
        val name = Messages.showInputDialog(project, "Topic name:", "Create Kafka Topic", null)
            ?.trim().orEmpty()
        if (name.isEmpty()) return
        val partsStr = Messages.showInputDialog(project, "Partitions:", "Create Kafka Topic", null, "3", null)
            ?.trim() ?: return
        val rfStr = Messages.showInputDialog(project, "Replication factor:", "Create Kafka Topic", null, "1", null)
            ?.trim() ?: return

        val parts = partsStr.toIntOrNull() ?: 1
        val rf = rfStr.toShortOrNull() ?: 1

        ApplicationManager.getApplication().executeOnPooledThread {
            try {
                KafkaClientService.createTopic(cluster, name, parts, rf)
                SwingUtilities.invokeLater { refreshCluster() }
            } catch (e: Exception) {
                SwingUtilities.invokeLater {
                    Messages.showErrorDialog(project, "Create topic failed: ${e.cause?.message ?: e.message}", "Error")
                }
            }
        }
    }

    private fun deleteSelectedTopic() {
        val cluster = selectedCluster() ?: return
        val idx = topicList.selectedIndex
        if (idx !in filteredTopics.indices) return
        val topic = filteredTopics[idx]

        val confirm = Messages.showYesNoDialog(
            project,
            "Delete topic '${topic.name}'? This cannot be undone.",
            "Delete Topic",
            Messages.getWarningIcon()
        )
        if (confirm != Messages.YES) return

        ApplicationManager.getApplication().executeOnPooledThread {
            try {
                KafkaClientService.deleteTopic(cluster, topic.name)
                SwingUtilities.invokeLater { refreshCluster() }
            } catch (e: Exception) {
                SwingUtilities.invokeLater {
                    Messages.showErrorDialog(project, "Delete topic failed: ${e.cause?.message ?: e.message}", "Error")
                }
            }
        }
    }
}
