package org.esgi.project.streaming

import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.state.{KeyValueStore, ValueAndTimestamp}
import org.apache.kafka.streams.test.TestRecord
import org.apache.kafka.streams.{KeyValue, TopologyTestDriver}
import org.esgi.project.streaming.StreamProcessingSpec.Utils.{generateTitle, generateViewCategory}
import org.scalatest.funsuite.AnyFunSuite
import org.esgi.project.streaming.models.{LikeEvent, MovieScore, MovieStats, ViewEvent}

import java.time.OffsetDateTime
import java.util.UUID
import scala.jdk.CollectionConverters._
import scala.util.Random

object StreamProcessingSpec {
  object Models {
    case class GeneratedEvent(view: ViewEvent, like: LikeEvent)
  }

  object Utils {
    def generateTitle: String = {
      val titles = List("Movie 1", "Movie 2", "Movie 3", "Movie 4")
      titles(Random.nextInt(titles.length))
    }

    def generateViewCategory: String = {
      val categories = List("start_only", "half", "full")
      categories(Random.nextInt(categories.length))
    }
  }

  object Converters {
    implicit class ViewEventToTestRecord(view: ViewEvent) {
      def toTestRecord: TestRecord[String, ViewEvent] =
        new TestRecord[String, ViewEvent](view.id.toString, view, view.timestamp.toInstant)
    }

    implicit class LikeEventToTestRecord(like: LikeEvent) {
      def toTestRecord: TestRecord[String, LikeEvent] =
        new TestRecord[String, LikeEvent](like.id.toString, like, like.timestamp.toInstant)
    }
  }
}

class StreamProcessingSpec extends AnyFunSuite with PlayJsonSupport {
  import StreamProcessingSpec.Converters._
  import StreamProcessingSpec.Models._
  import StreamProcessingSpec.Utils

  test("Validate statistics computation for movies") {
    // Given
    val ids = List(1, 2, 3, 4, 5)

    val generatedEvents: List[GeneratedEvent] = ids.flatMap { id =>
      val count = 5 + Random.nextInt(25)
      (1 to count).map { _ =>
        val title = generateTitle
        val category = generateViewCategory
        val viewEvent = ViewEvent(id, title, category, OffsetDateTime.now())
        val likeEvent = LikeEvent(id, Random.nextDouble() * 5, OffsetDateTime.now())
        GeneratedEvent(viewEvent, likeEvent)
      }
    }

    val views = generatedEvents.map(_.view)
    val likes = generatedEvents.map(_.like)

    // When
    val testDriver: TopologyTestDriver =
      new TopologyTestDriver(StreamProcessing.topology, StreamProcessing.buildProperties)
    val viewPipe = testDriver.createInputTopic(
      StreamProcessing.viewsTopic,
      Serdes.stringSerde.serializer,
      toSerde[ViewEvent].serializer
    )
    val likePipe = testDriver.createInputTopic(
      StreamProcessing.likesTopic,
      Serdes.stringSerde.serializer,
      toSerde[LikeEvent].serializer
    )

    viewPipe.pipeRecordList(views.map(_.toTestRecord).asJava)
    likePipe.pipeRecordList(likes.map(_.toTestRecord).asJava)

    // Then
    // Assert the count of views per category
    val viewCounts = views.groupBy(_.id).mapValues { events =>
      events.groupBy(_.view_category).mapValues(_.size.toLong)
    }

    val viewCountStore: KeyValueStore[Int, MovieStats] =
      testDriver.getKeyValueStore[Int, MovieStats](StreamProcessing.viewCountStoreName)

    viewCounts.foreach { case (id, counts) =>
      val storedCounts = viewCountStore.get(id)
      assert(storedCounts.startOnly == counts.getOrElse("start_only", 0L))
      assert(storedCounts.half == counts.getOrElse("half", 0L))
      assert(storedCounts.full == counts.getOrElse("full", 0L))
    }

    // Assert the average score per movie
    val averageScores = likes.groupBy(_.id).mapValues { events =>
      events.map(_.score).sum / events.size
    }

    val scoreStore: KeyValueStore[Int, MovieScore] =
      testDriver.getKeyValueStore[Int, MovieScore](StreamProcessing.scoreStoreName)

    averageScores.foreach { case (id, avgScore) =>
      val storedAvgScore = scoreStore.get(id)
      assert(storedAvgScore.averageScore == avgScore)
    }
  }
}






//package streaming
//
//import io.github.azhur.kafka.serde.PlayJsonSupport
//import org.apache.kafka.streams.scala.serialization.Serdes
//import org.apache.kafka.streams.state.{ValueAndTimestamp, WindowStore}
//import org.apache.kafka.streams.test.TestRecord
//import org.apache.kafka.streams.{KeyValue, TopologyTestDriver}
//import org.esgi.project.streaming.StreamProcessing
//import org.scalatest.funsuite.AnyFunSuite
////import streaming.models.{MeanLatencyForURL, Metric, Visit}
//import  org.esgi.project.streaming.models.{MeanLatencyForURL, Metric, Visit}
////import org.esgi.project.java.streaming.StreamProcessing
//import java.lang
//import java.time.OffsetDateTime
//import java.time.temporal.ChronoUnit
//import java.util.UUID
//import scala.jdk.CollectionConverters._
//import scala.util.Random
//
//object StreamProcessingSpec {
//  object Models {
//    case class GeneratedVisit(visit: Visit, metric: Metric)
//  }
//
//  object Utils {
//    def generateIpAddress: String = {
//      val random = new Random()
//      val ip = new Array[Int](4)
//      ip(0) = random.nextInt(256)
//      ip(1) = random.nextInt(256)
//      ip(2) = random.nextInt(256)
//      ip(3) = random.nextInt(256)
//      ip.mkString(".")
//    }
//  }
//
//  object Converters {
//    implicit class VisitToTestRecord(visit: Visit) {
//      def toTestRecord: TestRecord[String, Visit] =
//        new TestRecord[String, Visit](visit._id, visit, visit.timestamp.toInstant)
//    }
//
//    implicit class MetricToTestRecord(metric: Metric) {
//      def toTestRecord: TestRecord[String, Metric] =
//        new TestRecord[String, Metric](metric._id, metric, metric.timestamp.toInstant)
//    }
//  }
//}
//
//class StreamProcessingSpec extends AnyFunSuite with PlayJsonSupport {
//  import StreamProcessingSpec.Converters._
//  import StreamProcessingSpec.Models._
//  import StreamProcessingSpec.Utils
//
//  test("Validate advanced statistics computation") {
//    // Given
//    val urls = List(
//      "/store",
//      "/store/tech/tv",
//      "/store/photo",
//      "/store/tech/phone",
//      "/store/photo/camera",
//      "/store/home-automation/energy",
//      "/store/home-automation/heating"
//    )
//
//    val generatedEvents: List[GeneratedVisit] = urls.flatMap { url =>
//      val count = 5 + Random.nextInt(25)
//      (1 to count)
//        .map { _ =>
//          val _id = UUID.randomUUID.toString
//          val ts = OffsetDateTime.now()
//          GeneratedVisit(
//            visit = Visit(_id, ts, Utils.generateIpAddress, url),
//            metric = Metric(_id, ts, Random.nextInt(2000))
//          )
//        }
//    }
//
//    val visits = generatedEvents.map(_.visit)
//    val metrics = generatedEvents.map(_.metric)
//
//    // When
//    val testDriver: TopologyTestDriver =
//      new TopologyTestDriver(StreamProcessing.topology, StreamProcessing.buildStreamsProperties)
//    val visitPipe = testDriver.createInputTopic(
//      StreamProcessing.visitsTopicName,
//      Serdes.stringSerde.serializer,
//      toSerde[Visit].serializer
//    )
//    val metricPipe = testDriver.createInputTopic(
//      StreamProcessing.metricsTopicName,
//      Serdes.stringSerde.serializer,
//      toSerde[Metric].serializer
//    )
//
//    visitPipe.pipeRecordList(visits.map(_.toTestRecord).asJava)
//    metricPipe.pipeRecordList(metrics.map(_.toTestRecord).asJava)
//
//    // Then
//    // Assert the count of visits per category in the last 30 seconds
//    val visitsPerCategory: Map[String, Int] = visits
//      .filter(_.url.startsWith("/store/"))
//      .groupBy(_.url.split("/")(2))
//      .map { case (category, visits) => (category, visits.size) }
//
//    val visitsPerCategoryBucketedPerMinute: WindowStore[String, ValueAndTimestamp[Long]] =
//      testDriver.getTimestampedWindowStore[String, Long](StreamProcessing.visitsPerCategoryBucketedPerMinuteStoreName)
//
//    visitsPerCategory.foreach { case (category, count) =>
//      val row: List[KeyValue[lang.Long, ValueAndTimestamp[Long]]] =
//        visitsPerCategoryBucketedPerMinute
//          .fetch(
//            category,
//            visits.head.timestamp.truncatedTo(ChronoUnit.MINUTES).toInstant,
//            visits.last.timestamp.truncatedTo(ChronoUnit.MINUTES).toInstant
//          )
//          .asScala
//          .toList
//      row.headOption match {
//        case Some(row) => assert(row.value.value() == count)
//        case None      => assert(false, s"No data for $category in ${visitsPerCategoryBucketedPerMinute.name()}")
//      }
//    }
//
//    // Assert the average latency per URL in the last 30 seconds
//    val averageLatencyPerUrl: Map[String, Long] = generatedEvents
//      .groupBy(_.visit.url)
//      .map { case (url, events) =>
//        val meanLatency = events.map(_.metric.latency).sum / events.size
//        (url, meanLatency)
//      }
//
//    val averageLatencyPerUrlBucketedPerMinute: WindowStore[String, ValueAndTimestamp[MeanLatencyForURL]] =
//      testDriver.getTimestampedWindowStore[String, MeanLatencyForURL](
//        StreamProcessing.AverageLatencyPerUrlBucketedPerMinuteStoreName
//      )
//
//    averageLatencyPerUrl.foreach { case (url, meanLatency) =>
//      val row: List[KeyValue[lang.Long, ValueAndTimestamp[MeanLatencyForURL]]] =
//        averageLatencyPerUrlBucketedPerMinute
//          .fetch(
//            url,
//            visits.head.timestamp.truncatedTo(ChronoUnit.MINUTES).toInstant,
//            visits.last.timestamp.truncatedTo(ChronoUnit.MINUTES).toInstant
//          )
//          .asScala
//          .toList
//      row.headOption match {
//        case Some(row) => assert(row.value.value().meanLatency == meanLatency)
//        case None      => assert(false, s"No data for $url in ${averageLatencyPerUrlBucketedPerMinute.name()}")
//      }
//    }
//
//  }
//}
//
//
////package org.esgi.project.streaming
////
////import io.github.azhur.kafka.serde.PlayJsonSupport
////import org.apache.kafka.streams.TopologyTestDriver
////import org.apache.kafka.streams.scala.serialization.Serdes
////import org.apache.kafka.streams.state.KeyValueStore
////import org.apache.kafka.streams.test.TestRecord
////import org.scalatest.funsuite.AnyFunSuite
////
////import scala.jdk.CollectionConverters._
////
////class StreamProcessingSpec extends AnyFunSuite with PlayJsonSupport {
////  test("Topology should compute a correct word count") {
////    // Given
////    val messages = List(
////      "hello world",
////      "hello moon",
////      "foobar",
////      "42"
////    )
////
////    val topologyTestDriver = new TopologyTestDriver(
////      StreamProcessing.builder.build(),
////      StreamProcessing.buildProperties
////    )
////
////    val wordTopic = topologyTestDriver
////      .createInputTopic(
////        StreamProcessing.wordTopic,
////        Serdes.stringSerde.serializer(),
////        Serdes.stringSerde.serializer()
////      )
////
////    val wordCountStore: KeyValueStore[String, Long] =
////      topologyTestDriver
////        .getKeyValueStore[String, Long](
////          StreamProcessing.wordCountStoreName
////        )
////
////    // When
////    wordTopic.pipeRecordList(
////      messages.map(message => new TestRecord(message, message)).asJava
////    )
////
////    // Then
////    assert(wordCountStore.get("hello") == 2)
////    assert(wordCountStore.get("world") == 1)
////    assert(wordCountStore.get("moon") == 1)
////    assert(wordCountStore.get("foobar") == 1)
////    assert(wordCountStore.get("42") == 1)
////  }
////}
