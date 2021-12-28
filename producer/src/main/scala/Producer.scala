import com.danielasfregola.twitter4s.TwitterStreamingClient
import com.danielasfregola.twitter4s.entities.Tweet
import com.typesafe.config.ConfigFactory
import scala.jdk.CollectionConverters._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import java.util.{Date, Properties}
import com.danielasfregola.twitter4s.entities.AccessToken
import com.danielasfregola.twitter4s.entities.ConsumerToken
import com.danielasfregola.twitter4s.entities.enums.Language
import com.danielasfregola.twitter4s.TwitterRestClient
import com.danielasfregola.twitter4s.entities.GeoBoundingBox
import com.danielasfregola.twitter4s.entities.Coordinate
import java.util.Calendar

object Producer extends App {
  val config = ConfigFactory.load()
  val searches = config.getConfigList("twitter.searches")

  val topic = "tweets"
  val brokers = "localhost:9092"

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  props.put(ProducerConfig.CLIENT_ID_CONFIG, "Producer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](props)

  val accessToken = AccessToken(
    config.getString("twitter.access.key"),
    config.getString("twitter.access.secret")
  )
  val consumerToken = ConsumerToken(
    config.getString("twitter.consumer.key"),
    config.getString("twitter.consumer.secret")
  )

  val streamingClient = TwitterStreamingClient(consumerToken, accessToken)
    streamingClient.filterStatuses(languages = Seq(Language.English), tracks=Seq("movie", "#movie")) {
      case tweet: Tweet => {
        val text = tweet.text.replaceAll("(RT|\\@\\w+\\:?)", "").trim()
        val data = new ProducerRecord[String, String](topic, null, Calendar.getInstance().getTime().getTime, "null", text)
        producer.send(data)
        print(data + "\n")
      }
    }
}
