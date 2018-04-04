import java.lang.StringBuffer
import java.text.SimpleDateFormat
import java.util
import java.util.Properties
import java.util.stream.Collectors

import net.liftweb.json._
import org.apache.commons.configuration.ConfigurationFactory.ConfigurationBuilder

import scala.collection.JavaConverters
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import twitter4j.{FilterQuery, Status, TwitterStreamFactory}
import utils.PropertiesLoader

import scala.collection.immutable.HashMap
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scalaj.http.{Http, HttpResponse}


object TweetGenerator extends Serializable {

  var hasRelatedKeywords = false
  def main(args: Array[String]): Unit = {

    //Inställningar för twitter
    System.setProperty("twitter4j.oauth.consumerKey", "pgd3EgAAYy0NsHfc6TD5XA4m0")
    System.setProperty("twitter4j.oauth.consumerSecret", "cp7LR8o4FQTc72cszuFoiQP0BQcEkpgoOHMqMn0mSLu6KFoxek")
    System.setProperty("twitter4j.oauth.accessToken", "384519993-dSTbfXUJe2FOaAnxPdw22i1s4QFj83MWtgBFMhZs")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "W3uxu0BdpqIhuByjaa2xWXm2Ae6yFuIofI4dTyKzz87wa")


    val sparkContext = new SparkContext()
    val stopEvent = false
    var streamingContext = Option.empty[StreamingContext]
    var shouldReload = false

    val zkQuorum = "localhost:2181"
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
    val group = "my-group"
    val twitterTopic = Set("twitterdata")
    val numThreads = 1
    //val Array(zkQuorum, group, topics, numThreads) = args;
    val sparkConf = new SparkConf().setAppName("TweetGenerator")
    //val ssc = new StreamingContext(sparkConf, Seconds(2))
    //ssc.sparkContext.setLogLevel("OFF")



    val processThread = new Thread() {
      override def run(): Unit = {
        while (!stopEvent) {
          if (streamingContext.isEmpty && !shouldReload) {
            streamingContext = Option(new StreamingContext(sparkContext, Seconds(1)))
            streamingContext.get.sparkContext.setLogLevel("OFF")

            val kafkaTweets = KafkaUtils.createStream(streamingContext.get, zkQuorum, group, Map("twitterdata" -> 1)).map(_._2)
            //val kafkaTweets = TwitterUtils.createStream(streamingContext.get, None)
            val kafkaKeywords = KafkaUtils.createStream(streamingContext.get, zkQuorum, group, Map("keyworddata" -> 1)).map(_._2)
            //val kafkaKeywords = streamingContext.get.socketTextStream("localhost", 8888)
            val words = kafkaKeywords.flatMap(_.split(" ")).toString
            kafkaKeywords.print()
            val filter = "Trump"

            //val relatedKeywords = getRelatedKeywords(words)

            val filterTweets = kafkaTweets.filter(_.contains(filter))
            filterTweets.print()
            streamingContext.get.start()
          } else {
            if (shouldReload) {
              streamingContext.get.stop(false, true)
              streamingContext.get.awaitTermination()
              streamingContext = Option.empty[StreamingContext]
              shouldReload = false
            } else {
              Thread.sleep(1000)
            }
          }
        }

        streamingContext.get.stop(true, true)
        streamingContext.get.awaitTermination()
      }
    }

    processThread.start()
    processThread.join()


    //Inställningar för Kafka

/*
    //Inställningar för elasticsearch
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.resource", "visualization/tweets")
    sparkConf.set("es.nodes", "localhost")
    sparkConf.set("es.port", "9200")
    sparkConf.set("es.nodes.discovery", "true")
    sparkConf.set("spark.serializer", classOf[KryoSerializer].getName)
    val index_name = "twitter"*/

    /*
    //Kafka topic för att ta emot Tweets
    val kafkaTweets = KafkaUtils.createStream(ssc, zkQuorum, group, Map("twitterdata" -> 1)).map(_._2)

    //Kafka topic för att ta emot keywords
    val kafkaKeywords = KafkaUtils.createStream(ssc, zkQuorum, group, Map("keyworddata" -> 1)).map(_._2)

    //kafkaKeywords.print()

    val words = kafkaKeywords.flatMap(_.split(" "))




    //Hämta ut relaterade ord för varje keyword från Google
    val relatedKeywords = getRelatedKeywords(words)

    kafkaKeywords.foreachRDD(rdd => {
      println("Here now")
      //ssc.stop(false, true)
      val kafkaTweetsNew = KafkaUtils.createStream(ssc, zkQuorum, group, Map("twitterdata" -> 1)).map(_._2)
      kafkaTweetsNew.filter(_.contains(relatedKeywords))
      kafkaTweetsNew.print()
      hasRelatedKeywords = false
    })
*/






    //val testArray = new ArrayBuffer[String]()
    //testArray.append("Obama")

    //Hämtar ut Tweets direkt från Twitter. Fördelen är att man kan sätta upp en HashMap som kan användas i Elasticsearch.
    /*
    val query = new FilterQuery()
    val filter = finalArray.mkString(",")
    query.track(filter)

    val tweets = TwitterUtils.createFilteredStream(ssc, None, Some(query))

    val tweetMap = tweets.map(status => {
      finalArray.foreach(word => {
        println("Contains keyword: " + (status.getText.contains(word) || status.getHashtagEntities().map(_.getText).contains(word)))
      })
      val hashtags = status.getHashtagEntities().map(_.getText)
      val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

      HashMap(
        //"userID" -> status.getUser.getId(),
        //"userScreenName" -> status.getUser.getScreenName(),
        "userName" -> status.getUser.getName(),
        //"userDescription" -> status.getUser.getDescription(),
        "message" -> status.getText(),
        //"messageLength" -> status.getText.length(),
        "hashtags" -> hashtags.mkString(" ")
        //"createdAt" -> formatter.format(status.getCreatedAt.getTime()),
        //"friendsCount" -> status.getUser.getFriendsCount(),
        //"followersCount" -> status.getUser.getFollowersCount(),
        //"coordinates" -> Option(status.getGeoLocation).map(geo => {s"${geo.getLatitude},${geo.getLongitude}"}),
        //"placeCountry" -> Option(status.getPlace).map(place => {s"${place.getCountry}"}),
        //"userLanguage" -> status.getUser.getLang,
        //"statusLanguage" -> status.getLang
      )
    })*/


    //ssc.start()
    //ssc.awaitTermination()
  }

  def getGoogleList(keyword: String): List[String] = {
    //println("Keyword:" + keyword)
    val response: HttpResponse[String] = Http("https://kgsearch.googleapis.com/v1/entities:search")
      .params(Seq("query" -> keyword.toString, "limit" -> 10.toString, "key" -> "AIzaSyAR0679Of_1TcUWQhgQS-_7hYSSv3SnE8s"))
      .asString

    val json = parse(response.body)
    val listObject = json \\ "name"
    val list: List[String] = listObject \\ classOf[JString]
    list
  }

  def getRelatedKeywords(words: DStream[String]): List[String] = {
    val wordArray = new ArrayBuffer[String]()
    val finalArray = new ArrayBuffer[String]()
    words.foreachRDD(rdd => if (!rdd.isEmpty()) {
      wordArray ++= rdd.collect()
      println("Array: " + wordArray)
      wordArray.foreach(word => {
        finalArray.clear()
        val returnedList = getGoogleList(word)
        returnedList.foreach(term => {
          if (!wordArray.contains(term)) {
            finalArray.append(term)
          }
        })
        println("ArrayFinal: " + finalArray)
        hasRelatedKeywords = true

      })
    })
    finalArray.toList
  }



}



