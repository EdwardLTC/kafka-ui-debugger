package dev.edward.kafkadebugger.ui

import com.intellij.openapi.application.ApplicationManager
import com.intellij.openapi.project.Project
import com.intellij.openapi.ui.ComboBox
import com.intellij.openapi.ui.DialogWrapper
import com.intellij.openapi.ui.ValidationInfo
import com.intellij.ui.components.JBLabel
import com.intellij.ui.components.JBPasswordField
import com.intellij.ui.components.JBTextField
import com.intellij.util.ui.FormBuilder
import com.intellij.util.ui.JBUI
import dev.edward.kafkadebugger.kafka.KafkaClientService
import dev.edward.kafkadebugger.model.ClusterConfig
import dev.edward.kafkadebugger.model.SaslMechanism
import dev.edward.kafkadebugger.model.SecurityProtocol
import java.awt.BorderLayout
import java.awt.FlowLayout
import javax.swing.*

class ClusterDialog(
    project: Project,
    existing: ClusterConfig? = null
) : DialogWrapper(project) {

    private val nameField = JBTextField(existing?.name ?: "New Kafka Cluster")
    private val bootstrapField = JBTextField(existing?.bootstrapServers ?: "localhost:9092")
    private val protocolCombo = ComboBox(SecurityProtocol.entries.toTypedArray()).apply {
        selectedItem = existing?.securityProtocol ?: SecurityProtocol.PLAINTEXT
    }
    private val saslMechCombo = ComboBox(SaslMechanism.entries.toTypedArray()).apply {
        selectedItem = existing?.saslMechanism ?: SaslMechanism.PLAIN
    }
    private val saslUserField = JBTextField(existing?.saslUsername ?: "")
    private val saslPassField = JBPasswordField().apply {
        text = existing?.saslPassword ?: ""
    }
    private val truststorePathField = JBTextField(existing?.sslTruststoreLocation ?: "")
    private val truststorePassField = JBPasswordField().apply {
        text = existing?.sslTruststorePassword ?: ""
    }
    private val extraPropsArea = JTextArea(
        existing?.extraProperties?.entries?.joinToString("\n") { "${it.key}=${it.value}" } ?: "",
        4, 36
    )
    private val testStatusLabel = JBLabel(" ")
    private val testButton = JButton("Test Connection")

    private val editingId = existing?.id

    init {
        title = if (existing == null) "Add Kafka Cluster" else "Edit Kafka Cluster"
        init()
        updateFieldVisibility()
        protocolCombo.addActionListener { updateFieldVisibility() }
        testButton.addActionListener { runConnectionTest() }
    }

    private fun updateFieldVisibility() {
        val proto = protocolCombo.selectedItem as SecurityProtocol
        val isSasl = proto == SecurityProtocol.SASL_PLAINTEXT || proto == SecurityProtocol.SASL_SSL
        val isSsl = proto == SecurityProtocol.SSL || proto == SecurityProtocol.SASL_SSL

        saslMechCombo.isEnabled = isSasl
        saslUserField.isEnabled = isSasl
        saslPassField.isEnabled = isSasl
        truststorePathField.isEnabled = isSsl
        truststorePassField.isEnabled = isSsl
    }

    override fun createCenterPanel(): JComponent {
        val form = FormBuilder.createFormBuilder()
            .addLabeledComponent("Cluster Name:", nameField, 1, false)
            .addLabeledComponent("Bootstrap Servers:", bootstrapField, 1, false)
            .addLabeledComponent("Security Protocol:", protocolCombo, 1, false)
            .addSeparator()
            .addLabeledComponent("SASL Mechanism:", saslMechCombo, 1, false)
            .addLabeledComponent("SASL Username:", saslUserField, 1, false)
            .addLabeledComponent("SASL Password:", saslPassField, 1, false)
            .addSeparator()
            .addLabeledComponent("SSL Truststore Path:", truststorePathField, 1, false)
            .addLabeledComponent("SSL Truststore Password:", truststorePassField, 1, false)
            .addSeparator()
            .addLabeledComponent("Extra Properties (key=value):", JScrollPane(extraPropsArea), 1, false)
            .panel

        val testPanel = JPanel(FlowLayout(FlowLayout.LEFT, 0, 4)).apply {
            add(testButton)
            add(Box.createHorizontalStrut(10))
            add(testStatusLabel)
        }

        return JPanel(BorderLayout(0, 8)).apply {
            border = JBUI.Borders.empty(4)
            preferredSize = java.awt.Dimension(500, 440)
            add(form, BorderLayout.CENTER)
            add(testPanel, BorderLayout.SOUTH)
        }
    }

    override fun doValidate(): ValidationInfo? {
        if (nameField.text.isBlank()) return ValidationInfo("Cluster name is required", nameField)
        if (bootstrapField.text.isBlank()) return ValidationInfo("Bootstrap servers is required", bootstrapField)
        return null
    }

    fun buildClusterConfig(): ClusterConfig {
        val extra = linkedMapOf<String, String>()
        extraPropsArea.text.lines().forEach { line ->
            val trimmed = line.trim()
            if (trimmed.isNotEmpty() && !trimmed.startsWith("#")) {
                val idx = trimmed.indexOf('=')
                if (idx > 0) {
                    extra[trimmed.substring(0, idx).trim()] = trimmed.substring(idx + 1).trim()
                }
            }
        }
        return ClusterConfig(
            id = editingId ?: java.util.UUID.randomUUID().toString(),
            name = nameField.text.trim(),
            bootstrapServers = bootstrapField.text.trim(),
            securityProtocol = protocolCombo.selectedItem as SecurityProtocol,
            saslMechanism = saslMechCombo.selectedItem as SaslMechanism,
            saslUsername = saslUserField.text.trim(),
            saslPassword = String(saslPassField.password),
            sslTruststoreLocation = truststorePathField.text.trim(),
            sslTruststorePassword = String(truststorePassField.password),
            extraProperties = extra
        )
    }

    private fun runConnectionTest() {
        val cfg = buildClusterConfig()
        testButton.isEnabled = false
        testStatusLabel.text = "Testing..."
        ApplicationManager.getApplication().executeOnPooledThread {
            val result = KafkaClientService.testConnection(cfg)
            SwingUtilities.invokeLater {
                testButton.isEnabled = true
                testStatusLabel.text = if (result.success) {
                    "Connected (${result.brokerCount} brokers, ${result.latencyMs}ms)"
                } else {
                    "Failed: ${result.errorMessage?.take(80)}"
                }
            }
        }
    }
}
