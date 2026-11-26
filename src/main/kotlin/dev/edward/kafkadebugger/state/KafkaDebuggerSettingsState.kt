package dev.edward.kafkadebugger.state

import com.intellij.openapi.components.PersistentStateComponent
import com.intellij.openapi.components.Service
import com.intellij.openapi.components.State
import com.intellij.openapi.components.Storage
import com.intellij.openapi.project.Project
import dev.edward.kafkadebugger.model.ClusterConfig
import dev.edward.kafkadebugger.model.ProtoSourceConfig
import dev.edward.kafkadebugger.model.TopicProtoMapping

@Service(Service.Level.PROJECT)
@State(
    name = "KafkaDebuggerSettingsState",
    storages = [Storage("kafkaUiDebugger.xml")]
)
class KafkaDebuggerSettingsState : PersistentStateComponent<KafkaDebuggerSettingsState.State> {

    data class State(
        var clusters: MutableList<ClusterConfig> = mutableListOf(
            ClusterConfig(name = "Local Kafka", bootstrapServers = "localhost:9092")
        ),
        var protoSources: MutableList<ProtoSourceConfig> = mutableListOf(),
        var topicProtoMappings: MutableList<TopicProtoMapping> = mutableListOf(),
        var defaultConsumeLimit: Int = 100,
        var showInternalTopics: Boolean = false
    )

    private var myState = State()

    override fun getState(): State = myState

    override fun loadState(state: State) {
        myState = state
    }

    fun findMappingForTopic(topic: String): TopicProtoMapping? {
        return myState.topicProtoMappings.firstOrNull { mapping ->
            matchesPattern(mapping.topicPattern, topic)
        }
    }

    private fun matchesPattern(pattern: String, topic: String): Boolean {
        if (pattern.isBlank()) return false
        if (pattern == topic) return true
        val regexStr = buildString {
            append("^")
            for (ch in pattern) {
                when (ch) {
                    '*' -> append(".*")
                    '?' -> append(".")
                    '.', '+', '(', ')', '[', ']', '{', '}', '^', '$', '|', '\\' -> {
                        append('\\').append(ch)
                    }
                    else -> append(ch)
                }
            }
            append("$")
        }
        return Regex(regexStr).matches(topic)
    }

    companion object {
        fun getInstance(project: Project): KafkaDebuggerSettingsState =
            project.getService(KafkaDebuggerSettingsState::class.java)
    }
}
