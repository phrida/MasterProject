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
import org.apache.spark.SparkConf
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
  def main(args: Array[String]): Unit = {

    //Inställningar för twitter
    System.setProperty("twitter4j.oauth.consumerKey", "pgd3EgAAYy0NsHfc6TD5XA4m0")
    System.setProperty("twitter4j.oauth.consumerSecret", "cp7LR8o4FQTc72cszuFoiQP0BQcEkpgoOHMqMn0mSLu6KFoxek")
    System.setProperty("twitter4j.oauth.accessToken", "384519993-dSTbfXUJe2FOaAnxPdw22i1s4QFj83MWtgBFMhZs")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "W3uxu0BdpqIhuByjaa2xWXm2Ae6yFuIofI4dTyKzz87wa")

    //Inställningar för Kafka
    val zkQuorum = "localhost:2181"
    val group = "my-group"
    val topics = "twitterdata,keyworddata"
    val numThreads = 1
    //val Array(zkQuorum, group, topics, numThreads) = args;
    val sparkConf = new SparkConf().setAppName("TweetGenerator")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.sparkContext.setLogLevel("OFF")

    //Inställningar för elasticsearch
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.resource", "visualization/tweets")
    sparkConf.set("es.nodes", "localhost")
    sparkConf.set("es.port", "9200")
    sparkConf.set("es.nodes.discovery", "true")
    sparkConf.set("spark.serializer", classOf[KryoSerializer].getName)
    val index_name = "twitter"
    //val keywords = ssc.sparkContext.broadcast(loadKeyWordsFile())
    //val filter = keywords.value


    //Försök på att ta emot keyword genom subscribe istället för DStream
/*
    val properties = new Properties()
    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("group.id", "my-group")
    properties.put("key.deserializer", classOf[StringDeserializer])
    properties.put("value.deserializer", classOf[StringDeserializer])

    val kafkaConsumer = new KafkaConsumer[String, String](properties)
    kafkaConsumer.subscribe(util.Arrays.asList("keyworddata"))

    val keywords = kafkaConsumer.poll(10)

    println(keywords)*/


    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    //Kafka topic för att ta emot Tweets
    val kafkaTweets = KafkaUtils.createStream(ssc, zkQuorum, group, Map("twitterdata" -> 1)).map(_._2)

    //Kafka topic för att ta emot keywords
    val kafkaKeywords = KafkaUtils.createStream(ssc, zkQuorum, group, Map("keyworddata" -> 1)).map(_._2)

    kafkaKeywords.print()


    val wordArray = new ArrayBuffer[String]()
    val finalArray = new ArrayBuffer[String]()
    val words = kafkaKeywords.flatMap(_.split(" "))


    //Hämta ut relaterade ord för varje keyword från Google
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
      })
    })

    //Måste filtrera Twitter-strömmen på ett eller annat sätt
    kafkaTweets.filter(_.contains(finalArray))

    kafkaTweets.print()

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


    ssc.start()
    ssc.awaitTermination()
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



}



