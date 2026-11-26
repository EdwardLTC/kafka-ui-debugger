package dev.edward.kafkadebugger.ui

import com.intellij.openapi.fileChooser.FileChooserDescriptorFactory
import com.intellij.openapi.fileChooser.FileChooser
import com.intellij.openapi.project.Project
import com.intellij.openapi.ui.DialogWrapper
import com.intellij.ui.JBColor
import com.intellij.ui.components.JBLabel
import com.intellij.ui.components.JBScrollPane
import com.intellij.ui.table.JBTable
import com.intellij.util.ui.JBUI
import dev.edward.kafkadebugger.model.ProtoSourceConfig
import dev.edward.kafkadebugger.model.ProtoSourceType
import dev.edward.kafkadebugger.model.TopicProtoMapping
import dev.edward.kafkadebugger.proto.ProtoRegistryService
import dev.edward.kafkadebugger.state.KafkaDebuggerSettingsState
import java.awt.BorderLayout
import java.awt.Dimension
import java.awt.FlowLayout
import javax.swing.*
import javax.swing.table.DefaultTableModel

class ProtoSettingsDialog(private val project: Project) : DialogWrapper(project) {

    private val settings = KafkaDebuggerSettingsState.getInstance(project).state
    private val protoRegistry = ProtoRegistryService.getInstance(project)

    private val sourcesModel = object : DefaultTableModel(arrayOf("Type", "Path"), 0) {
        override fun isCellEditable(row: Int, column: Int) = false
    }
    private val sourcesTable = JBTable(sourcesModel)

    private val mappingsModel = object : DefaultTableModel(
        arrayOf("Topic Pattern (exact or *)", "Protobuf Message Full Name", "Strip Confluent Header"), 0
    ) {
        override fun getColumnClass(columnIndex: Int): Class<*> =
            if (columnIndex == 2) Boolean::class.javaObjectType else String::class.java
    }
    private val mappingsTable = JBTable(mappingsModel)

    private val statusLabel = JBLabel(" ")

    init {
        title = "Protobuf & Avro Schemas & Topic Mappings"
        loadFromState()
        init()
    }

    private fun loadFromState() {
        sourcesModel.rowCount = 0
        settings.protoSources.forEach { src ->
            sourcesModel.addRow(arrayOf(src.type.displayName, src.path))
        }
        mappingsModel.rowCount = 0
        settings.topicProtoMappings.forEach { m ->
            mappingsModel.addRow(arrayOf<Any?>(m.topicPattern, m.messageFullName, m.stripConfluentHeader))
        }
        val count = protoRegistry.getAvailableMessageTypes().size
        statusLabel.text = "Loaded $count Protobuf message types."
    }

    override fun createCenterPanel(): JComponent {
        val addDirBtn = JButton("+ .proto Dir").apply {
            addActionListener { addProtoDir() }
        }
        val addDescBtn = JButton("+ .desc / .pb").apply {
            addActionListener { addDescriptorFile() }
        }
        val addAvroFileBtn = JButton("+ .avsc File").apply {
            addActionListener { addAvroFile() }
        }
        val addAvroDirBtn = JButton("+ .avsc Dir").apply {
            addActionListener { addAvroDir() }
        }
        val removeSourceBtn = JButton("Remove").apply {
            addActionListener {
                val row = sourcesTable.selectedRow
                if (row >= 0) sourcesModel.removeRow(row)
            }
        }
        val reloadBtn = JButton("Reload Schemas").apply {
            addActionListener { reloadSchemasNow() }
        }

        val sourcesToolbar = JPanel(FlowLayout(FlowLayout.LEFT, 4, 2)).apply {
            add(addDirBtn)
            add(addDescBtn)
            add(addAvroFileBtn)
            add(addAvroDirBtn)
            add(removeSourceBtn)
            add(reloadBtn)
        }

        val sourcesPanel = JPanel(BorderLayout(0, 4)).apply {
            border = BorderFactory.createTitledBorder("1. Schema Sources (.proto, .desc/.pb, .avsc)")
            add(sourcesToolbar, BorderLayout.NORTH)
            add(JBScrollPane(sourcesTable).apply { preferredSize = Dimension(680, 140) }, BorderLayout.CENTER)
        }

        val addMapBtn = JButton("+ Add Mapping").apply {
            addActionListener {
                val types = protoRegistry.getAvailableMessageTypes()
                val defaultType = types.firstOrNull() ?: "com.example.MyMessage"
                mappingsModel.addRow(arrayOf<Any?>("my-topic*", defaultType, false))
            }
        }
        val removeMapBtn = JButton("Remove Mapping").apply {
            addActionListener {
                val row = mappingsTable.selectedRow
                if (row >= 0) mappingsModel.removeRow(row)
            }
        }
        val mappingsToolbar = JPanel(FlowLayout(FlowLayout.LEFT, 4, 2)).apply {
            add(addMapBtn)
            add(removeMapBtn)
        }

        val mappingsPanel = JPanel(BorderLayout(0, 4)).apply {
            border = BorderFactory.createTitledBorder("2. Topic -> Protobuf Message Mapping")
            add(mappingsToolbar, BorderLayout.NORTH)
            add(JBScrollPane(mappingsTable).apply { preferredSize = Dimension(680, 160) }, BorderLayout.CENTER)
        }

        return JPanel(BorderLayout(0, 8)).apply {
            border = JBUI.Borders.empty(4)
            preferredSize = Dimension(720, 450)
            add(sourcesPanel, BorderLayout.NORTH)
            add(mappingsPanel, BorderLayout.CENTER)
            add(statusLabel, BorderLayout.SOUTH)
        }
    }

    private fun addProtoDir() {
        val desc = FileChooserDescriptorFactory.createSingleFolderDescriptor().apply {
            title = "Select .proto Root Directory"
        }
        FileChooser.chooseFile(desc, project, null) { vf ->
            sourcesModel.addRow(arrayOf(ProtoSourceType.PROTO_DIR.displayName, vf.path))
            reloadSchemasNow()
        }
    }

    private fun addDescriptorFile() {
        val desc = com.intellij.openapi.fileChooser.FileChooserDescriptor(true, false, false, false, false, false).apply {
            title = "Select .desc / .pb FileDescriptorSet"
        }
        FileChooser.chooseFile(desc, project, null) { vf ->
            sourcesModel.addRow(arrayOf(ProtoSourceType.DESCRIPTOR_SET.displayName, vf.path))
            reloadSchemasNow()
        }
    }

    private fun addAvroFile() {
        val desc = com.intellij.openapi.fileChooser.FileChooserDescriptor(true, false, false, false, false, false).apply {
            title = "Select .avsc Avro Schema File"
        }
        FileChooser.chooseFile(desc, project, null) { vf ->
            sourcesModel.addRow(arrayOf(ProtoSourceType.AVRO_FILE.displayName, vf.path))
            reloadSchemasNow()
        }
    }

    private fun addAvroDir() {
        val desc = FileChooserDescriptorFactory.createSingleFolderDescriptor().apply {
            title = "Select .avsc Root Directory"
        }
        FileChooser.chooseFile(desc, project, null) { vf ->
            sourcesModel.addRow(arrayOf(ProtoSourceType.AVRO_DIR.displayName, vf.path))
            reloadSchemasNow()
        }
    }

    private fun collectSourcesFromTable(): List<ProtoSourceConfig> {
        val list = mutableListOf<ProtoSourceConfig>()
        for (i in 0 until sourcesModel.rowCount) {
            val typeStr = sourcesModel.getValueAt(i, 0).toString()
            val path = sourcesModel.getValueAt(i, 1).toString()
            val type = ProtoSourceType.entries.firstOrNull { it.displayName == typeStr }
                ?: ProtoSourceType.PROTO_DIR
            list.add(ProtoSourceConfig(path = path, type = type))
        }
        return list
    }

    private fun reloadSchemasNow() {
        val sources = collectSourcesFromTable()
        val errors = protoRegistry.reload(sources)
        val count = protoRegistry.getAvailableMessageTypes().size
        if (errors.isEmpty()) {
            statusLabel.foreground = JBColor.foreground()
            statusLabel.text = "Loaded $count Protobuf message types successfully."
        } else {
            statusLabel.foreground = JBColor.RED
            statusLabel.text = "Loaded $count types with errors: ${errors.first()}"
        }
    }

    override fun doOKAction() {
        if (mappingsTable.isEditing) {
            mappingsTable.cellEditor?.stopCellEditing()
        }
        settings.protoSources.clear()
        settings.protoSources.addAll(collectSourcesFromTable())

        settings.topicProtoMappings.clear()
        for (i in 0 until mappingsModel.rowCount) {
            val pattern = mappingsModel.getValueAt(i, 0)?.toString()?.trim() ?: ""
            val msgName = mappingsModel.getValueAt(i, 1)?.toString()?.trim() ?: ""
            val strip = mappingsModel.getValueAt(i, 2) as? Boolean ?: false
            if (pattern.isNotBlank() && msgName.isNotBlank()) {
                settings.topicProtoMappings.add(TopicProtoMapping(pattern, msgName, strip))
            }
        }
        protoRegistry.reload(settings.protoSources)
        super.doOKAction()
    }
}
