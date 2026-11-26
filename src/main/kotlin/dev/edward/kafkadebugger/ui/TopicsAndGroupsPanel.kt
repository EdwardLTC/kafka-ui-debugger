package dev.edward.kafkadebugger.ui

import com.intellij.openapi.application.ApplicationManager
import com.intellij.openapi.ui.Messages
import com.intellij.ui.JBSplitter
import com.intellij.ui.components.JBLabel
import com.intellij.ui.components.JBScrollPane
import com.intellij.ui.table.JBTable
import com.intellij.util.ui.JBUI
import dev.edward.kafkadebugger.kafka.KafkaClientService
import dev.edward.kafkadebugger.model.ClusterConfig
import dev.edward.kafkadebugger.model.ConsumerGroupInfo
import dev.edward.kafkadebugger.model.TopicInfo
import java.awt.BorderLayout
import java.awt.FlowLayout
import javax.swing.*
import javax.swing.table.DefaultTableModel

class TopicInspectorPanel : JPanel(BorderLayout()) {

    private val headerLabel = JBLabel("Select a topic in the explorer to inspect partitions and configs.")
    private val partitionsModel = object : DefaultTableModel(
        arrayOf("Partition", "Leader", "Replicas", "ISR", "Begin Offset", "End Offset", "Messages"), 0
    ) {
        override fun isCellEditable(row: Int, column: Int) = false
    }
    private val configsModel = object : DefaultTableModel(arrayOf("Config Key", "Value"), 0) {
        override fun isCellEditable(row: Int, column: Int) = false
    }

    init {
        add(headerLabel.apply { border = JBUI.Borders.empty(6) }, BorderLayout.NORTH)
        val splitter = JBSplitter(true, 0.55f).apply {
            firstComponent = JPanel(BorderLayout()).apply {
                border = BorderFactory.createTitledBorder("Partitions")
                add(JBScrollPane(JBTable(partitionsModel)), BorderLayout.CENTER)
            }
            secondComponent = JPanel(BorderLayout()).apply {
                border = BorderFactory.createTitledBorder("Non-default Topic Configs")
                add(JBScrollPane(JBTable(configsModel)), BorderLayout.CENTER)
            }
        }
        add(splitter, BorderLayout.CENTER)
    }

    fun loadTopic(cluster: ClusterConfig, topicName: String) {
        headerLabel.text = "Loading $topicName..."
        ApplicationManager.getApplication().executeOnPooledThread {
            try {
                val detail: TopicInfo = KafkaClientService.getTopicDetail(cluster, topicName)
                SwingUtilities.invokeLater {
                    headerLabel.text =
                        "Topic: ${detail.name} | Partitions: ${detail.partitionCount} | RF: ${detail.replicationFactor} | Total Messages: ${detail.totalMessages}"
                    partitionsModel.rowCount = 0
                    detail.partitions.forEach { p ->
                        partitionsModel.addRow(
                            arrayOf<Any?>(
                                p.partition,
                                p.leaderId,
                                p.replicaIds.joinToString(","),
                                p.isrIds.joinToString(","),
                                p.beginningOffset,
                                p.endOffset,
                                p.messageCount
                            )
                        )
                    }
                    configsModel.rowCount = 0
                    detail.configs.forEach { (k, v) ->
                        configsModel.addRow(arrayOf(k, v))
                    }
                }
            } catch (e: Exception) {
                SwingUtilities.invokeLater {
                    headerLabel.text = "Failed to load $topicName: ${e.cause?.message ?: e.message}"
                }
            }
        }
    }
}

class ConsumerGroupsPanel : JPanel(BorderLayout()) {

    private var currentCluster: ClusterConfig? = null
    private var groups: List<ConsumerGroupInfo> = emptyList()

    private val statusLabel = JBLabel("Click Refresh to load consumer groups.")
    private val refreshBtn = JButton("Refresh")
    private val resetOffsetsBtn = JButton("Reset Offsets...")

    private val groupsModel = object : DefaultTableModel(
        arrayOf("Group ID", "State", "Coordinator", "Members", "Total Lag"), 0
    ) {
        override fun isCellEditable(row: Int, column: Int) = false
    }
    private val groupsTable = JBTable(groupsModel).apply {
        setSelectionMode(ListSelectionModel.SINGLE_SELECTION)
    }

    private val offsetsModel = object : DefaultTableModel(
        arrayOf("Topic", "Partition", "Committed Offset", "Log End Offset", "Lag"), 0
    ) {
        override fun isCellEditable(row: Int, column: Int) = false
    }

    private val membersModel = object : DefaultTableModel(
        arrayOf("Client ID", "Host", "Assigned Partitions"), 0
    ) {
        override fun isCellEditable(row: Int, column: Int) = false
    }

    init {
        val toolbar = JPanel(FlowLayout(FlowLayout.LEFT, 6, 4)).apply {
            add(refreshBtn)
            add(resetOffsetsBtn)
            add(statusLabel)
        }
        add(toolbar, BorderLayout.NORTH)

        val detailTabs = com.intellij.ui.components.JBTabbedPane().apply {
            addTab("Partition Offsets & Lag", JBScrollPane(JBTable(offsetsModel)))
            addTab("Active Members", JBScrollPane(JBTable(membersModel)))
        }

        val splitter = JBSplitter(true, 0.45f).apply {
            firstComponent = JBScrollPane(groupsTable)
            secondComponent = detailTabs
        }
        add(splitter, BorderLayout.CENTER)

        refreshBtn.addActionListener { refresh() }
        resetOffsetsBtn.addActionListener { promptResetOffsets() }
        groupsTable.selectionModel.addListSelectionListener { e ->
            if (!e.valueIsAdjusting) {
                val row = groupsTable.selectedRow
                if (row in groups.indices) {
                    showGroupDetail(groups[row])
                }
            }
        }
    }

    fun setCluster(cluster: ClusterConfig?) {
        currentCluster = cluster
        groups = emptyList()
        groupsModel.rowCount = 0
        offsetsModel.rowCount = 0
        membersModel.rowCount = 0
        if (cluster != null) refresh()
    }

    fun refresh() {
        val cluster = currentCluster ?: return
        statusLabel.text = "Loading consumer groups..."
        refreshBtn.isEnabled = false
        ApplicationManager.getApplication().executeOnPooledThread {
            try {
                val loaded = KafkaClientService.listConsumerGroups(cluster)
                SwingUtilities.invokeLater {
                    refreshBtn.isEnabled = true
                    groups = loaded
                    groupsModel.rowCount = 0
                    loaded.forEach { g ->
                        groupsModel.addRow(arrayOf<Any?>(g.groupId, g.state, g.coordinator, g.members.size, g.totalLag))
                    }
                    statusLabel.text = "Loaded ${loaded.size} consumer groups."
                }
            } catch (e: Exception) {
                SwingUtilities.invokeLater {
                    refreshBtn.isEnabled = true
                    statusLabel.text = "Error: ${e.cause?.message ?: e.message}"
                }
            }
        }
    }

    private fun showGroupDetail(group: ConsumerGroupInfo) {
        offsetsModel.rowCount = 0
        group.offsets.forEach { o ->
            offsetsModel.addRow(arrayOf<Any?>(o.topic, o.partition, o.committedOffset, o.logEndOffset, o.lag))
        }
        membersModel.rowCount = 0
        group.members.forEach { m ->
            membersModel.addRow(arrayOf(m.clientId, m.host, m.assignments.joinToString(", ")))
        }
    }

    private fun promptResetOffsets() {
        val cluster = currentCluster ?: return
        val row = groupsTable.selectedRow
        if (row !in groups.indices) {
            Messages.showInfoMessage("Select a consumer group first.", "Reset Offsets")
            return
        }
        val group = groups[row]
        val topics = group.offsets.map { it.topic }.distinct()
        val topic = if (topics.size == 1) {
            topics.first()
        } else {
            Messages.showInputDialog(
                "Enter topic name for group '${group.groupId}':",
                "Reset Offsets",
                null,
                topics.firstOrNull() ?: "",
                null
            ) ?: return
        }

        val choice = Messages.showDialog(
            this,
            "Reset offsets for group '${group.groupId}' on topic '$topic'?\n(Group must have no active consumers)",
            "Reset Offsets",
            arrayOf("Earliest", "Latest", "Cancel"),
            1,
            null
        )
        if (choice != 0 && choice != 1) return
        val toLatest = (choice == 1)

        statusLabel.text = "Resetting offsets..."
        ApplicationManager.getApplication().executeOnPooledThread {
            try {
                KafkaClientService.resetConsumerGroupOffsets(cluster, group.groupId, topic, toLatest)
                SwingUtilities.invokeLater {
                    statusLabel.text = "Offsets reset to ${if (toLatest) "LATEST" else "EARLIEST"}."
                    refresh()
                }
            } catch (e: Exception) {
                SwingUtilities.invokeLater {
                    Messages.showErrorDialog(
                        "Failed to reset offsets: ${e.cause?.message ?: e.message}",
                        "Reset Offsets Error"
                    )
                    statusLabel.text = "Reset failed."
                }
            }
        }
    }
}
