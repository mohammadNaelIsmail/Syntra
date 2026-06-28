package config
import com.typesafe.config.ConfigFactory

object AppConfig {
  private val conf = ConfigFactory.load()

  object Kafka {
    val bootstrapServers: String = conf.getString("kafka.bootstrap-servers")
    val topic: String            = conf.getString("kafka.topic")
  }

  object Elasticsearch {
    val nodes: String           = conf.getString("elasticsearch.nodes")
    val port: String            = conf.getString("elasticsearch.port")
    val snapshotIndex: String   = conf.getString("elasticsearch.indices.snapshot")
    val analyticsIndex: String  = conf.getString("elasticsearch.indices.analytics")
  }

  object Checkpoints {
    val snapshot: String  = conf.getString("checkpoints.snapshot")
    val analytics: String = conf.getString("checkpoints.analytics")
  }
}
