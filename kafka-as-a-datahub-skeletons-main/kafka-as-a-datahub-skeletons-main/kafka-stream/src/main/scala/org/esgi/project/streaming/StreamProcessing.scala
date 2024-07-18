package org.esgi.project.streaming

import io.github.azhur.kafka.serde.PlayJsonSupport
import io.github.azhur.kafka.serde.PlayJsonSupport.toSerde
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{KGroupedStream, KStream, KTable, Materialized}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.esgi.project.streaming.models.{LikeEvent, MovieScore, MovieStats, ViewEvent}

import java.util.Properties

object StreamProcessing extends PlayJsonSupport {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.serialization.Serdes._

  val applicationName = s"movie-stats-app-2"

  private val props: Properties = buildProperties

  // defining processing graph
  val builder: StreamsBuilder = new StreamsBuilder

  val viewsTopic = "views"
  val likesTopic = "likes"

  val viewCountStoreName = "view-count-store"
  val scoreStoreName = "score-store"

  val views: KStream[String, ViewEvent] = builder.stream[String, ViewEvent](viewsTopic)
  val likes: KStream[String, LikeEvent] = builder.stream[String, LikeEvent](likesTopic)

  // Aggregating views by category
  private val viewsByCategory: KGroupedStream[Int, ViewEvent] = views.groupBy((_, view) => view.id)
  val viewCounts: KTable[Int, MovieStats] = viewsByCategory.aggregate(MovieStats(0, 0, 0, 0))(
    (key, view, agg) => {
      val category = view.view_category
      category match {
        case "start_only" => agg.copy(totalViews = agg.totalViews + 1, startOnly = agg.startOnly + 1)
        case "half" => agg.copy(totalViews = agg.totalViews + 1, half = agg.half + 1)
        case "full" => agg.copy(totalViews = agg.totalViews + 1, full = agg.full + 1)
        case _ => agg
      }
    }

  )(Materialized.as(viewCountStoreName))

  // Calculating average scores
  private val likesByMovie: KGroupedStream[Int, LikeEvent] = likes.groupBy((_, like) => like.id)
  val averageScores: KTable[Int, MovieScore] = likesByMovie.aggregate(MovieScore(0.0, 0))(
    (key, like, agg) => MovieScore(agg.totalScore + like.score, agg.count + 1)
  )(Materialized.as(scoreStoreName))

  def run(): KafkaStreams = {
    val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
    streams.start()

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable() {
      override def run(): Unit = {
        streams.close()
      }
    }))
    streams
  }

  def buildProperties: Properties = {
    val properties = new Properties()
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.put(StreamsConfig.CLIENT_ID_CONFIG, applicationName)
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationName)
    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "-1")
    properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
    properties
  }

  def topology: Topology = builder.build()
}




//package org.esgi.project.streaming
//
//import io.github.azhur.kafka.serde.PlayJsonSupport
//import org.apache.kafka.clients.consumer.ConsumerConfig
//import org.apache.kafka.clients.producer.ProducerConfig
//import org.apache.kafka.streams.scala._
//import org.apache.kafka.streams.scala.kstream.{KTable, Materialized}
//import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
//
//import java.util.Properties
//
//object StreamProcessing extends PlayJsonSupport {
//
//  import org.apache.kafka.streams.scala.ImplicitConversions._
//  import org.apache.kafka.streams.scala.serialization.Serdes._
//
//  val applicationName = s"some-application-name"
//
//  private val props: Properties = buildProperties
//
//  // defining processing graph
//  val builder: StreamsBuilder = new StreamsBuilder
//
//  val wordTopic = "words"
//  val wordCountStoreName = "word-count-store"
//
//  val words = builder.stream[String, String](wordTopic)
//
//  val wordCounts: KTable[String, Long] = words
//    .flatMapValues(textLine => textLine.toLowerCase.split("\\W+"))
//    .groupBy((_, word) => word)
//    .count()(Materialized.as(wordCountStoreName))
//
//  def run(): KafkaStreams = {
//    val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
//    streams.start()
//
//    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
//    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable() {
//      override def run(): Unit = {
//        streams.close()
//      }
//    }))
//    streams
//  }
//
//  // auto loader from properties file in project
//  def buildProperties: Properties = {
//    val properties = new Properties()
//    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
//    properties.put(StreamsConfig.CLIENT_ID_CONFIG, applicationName)
//    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationName)
//    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0")
//    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
//    properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "-1")
//    properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
//    properties
//  }
//}
