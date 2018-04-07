import java.io._
import java.text.SimpleDateFormat

import twitter4j._
import net.liftweb.json._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.elasticsearch.spark.rdd.EsSpark

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scalaj.http.{Http, HttpResponse}

import scala.collection.immutable.HashMap
import scala.io.Source



object TweetGenerator extends Serializable {

  def main(args: Array[String]): Unit = {

    //Inställningar för twitter
    System.setProperty("twitter4j.oauth.consumerKey", "pgd3EgAAYy0NsHfc6TD5XA4m0")
    System.setProperty("twitter4j.oauth.consumerSecret", "cp7LR8o4FQTc72cszuFoiQP0BQcEkpgoOHMqMn0mSLu6KFoxek")
    System.setProperty("twitter4j.oauth.accessToken", "384519993-dSTbfXUJe2FOaAnxPdw22i1s4QFj83MWtgBFMhZs")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "W3uxu0BdpqIhuByjaa2xWXm2Ae6yFuIofI4dTyKzz87wa")


    val zkQuorum = "localhost:2181"
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
    val group = "my-group"
    val twitterTopic = Set("twitterdata")
    val numThreads = 1
    //val Array(zkQuorum, group, topics, numThreads) = args;
    val sparkConf = new SparkConf().setAppName("TweetGenerator").setMaster("local[*]")
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.resource", "visualization/tweets")
    sparkConf.set("es.nodes", "localhost")
    sparkConf.set("es.port", "9200")
    sparkConf.set("es.nodes.discovery", "true")
    val index_name = "twitter"
    var ssc = Option.empty[StreamingContext]
    ssc = Option(new StreamingContext(sparkConf, Seconds(2)))
    ssc.get.sparkContext.setLogLevel("OFF")

    val initialFile = new ListBuffer[String]
    initialFile.append("Initial")
    writeToFile(initialFile)
    println("Initial file: " + readFromFile())

    val filterLines = KafkaUtils.createStream(ssc.get, zkQuorum, group, Map("keyworddata" -> 1)).map(_._2)
    val filterWords = filterLines.flatMap(_.split(" "))
    filterWords.print()

    filterWords.foreachRDD(rdd => {
      rdd.foreach(word => {
        val words = new ListBuffer[String]

        println("Word to append: " + word)
        words.append(word)
        println("Words in ListBuffer: " + words)
        writeToFile(words)
        println("Words in file: " + readFromFile().toList)
      })
    })

    val tweets = KafkaUtils.createStream(ssc.get, zkQuorum, group, Map("twitterdata" -> 1)).map(_._2)

    //tweets.print()
    //val tweets = TwitterUtils.createStream(ssc.get, None)

    val filteredTweets = tweets.filter(_.contains(readFromFile().toList.head))

    //filteredTweets.print()

    val tweetMap = filteredTweets.map(json => {
      val status = TwitterObjectFactory.createStatus(json)
      val hashtags = status.getHashtagEntities().map(_.getText())
      val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
      //val sentiment = predictSentiment(status)
      //val translatedSentiment = translateSentiment(sentiment)


      HashMap(
        "userID" -> status.getUser.getId(),
        "userScreenName" -> status.getUser.getScreenName(),
        "userName" -> status.getUser.getName(),
        "userDescription" -> status.getUser.getDescription(),
        "message" -> status.getText(),
        "messageLength" -> status.getText.length(),
        "hashtags" -> hashtags.mkString(" "),
        "createdAt" -> formatter.format(status.getCreatedAt.getTime()),
        "friendsCount" -> status.getUser.getFriendsCount(),
        "followersCount" -> status.getUser.getFollowersCount(),
        "coordinates" -> Option(status.getGeoLocation).map(geo => {s"${geo.getLatitude},${geo.getLongitude}"}),
        "placeCountry" -> Option(status.getPlace).map(place => {s"${place.getCountry}"}),
        "userLanguage" -> status.getUser.getLang,
        "statusLanguage" -> status.getLang
        //"sentiment" -> translatedSentiment
      )
    })

    tweetMap.print()

    tweetMap.foreachRDD(tweet => EsSpark.saveToEs(tweet, "visualization/tweets"))


/*
    filteredTweets.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        //println("Status: " + partitionOfRecords.getClass)
        while (partitionOfRecords.hasNext) {
          val tweet = partitionOfRecords.next()
          //println("TweetOriginal: " + tweet)

          val status = TwitterObjectFactory.createStatus(tweet)
          //println("TweetStatus: " + status)


          val hashtags = status.getHashtagEntities().map(_.getText)
          val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

          val tweetMap = HashMap(
            "userID" -> status.getUser.getId(),
            "userScreenName" -> status.getUser.getScreenName(),
            "userName" -> status.getUser.getName(),
            "userDescription" -> status.getUser.getDescription(),
            "message" -> status.getText(),
            "messageLength" -> status.getText.length(),
            "hashtags" -> hashtags.mkString(" "),
            "createdAt" -> formatter.format(status.getCreatedAt.getTime()),
            "friendsCount" -> status.getUser.getFriendsCount(),
            "followersCount" -> status.getUser.getFollowersCount(),
            "coordinates" -> Option(status.getGeoLocation).map(geo => {s"${geo.getLatitude},${geo.getLongitude}"}),
            "placeCountry" -> Option(status.getPlace).map(place => {s"${place.getCountry}"}),
            "userLanguage" -> status.getUser.getLang,
            "statusLanguage" -> status.getLang
          )
          println("TweetMap: " + tweetMap)
        }

      })
    })*/

    //filteredTweets.foreachRDD { tweet => EsSpark.saveToEs(tweet, "visualization/tweets")}
    //tweetMap.foreachRDD{ tweet => EsSpark.saveToEs(tweet, "visualization/tweets")
    //filteredTweets.print()


    ssc.get.start()
    ssc.get.awaitTermination()

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
/*
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

      })
    })
    finalArray.toList
  }*/

  def writeToFile(list: ListBuffer[String]): Any = {

    val file = new File("/Users/Phrida/IdeaProjects/Masterprosjekt/src/main/scala/resources/output.txt")
    println(file.exists())
    val writer = new PrintWriter(file)
    //new FileOutputStream(file,true)
    //val finalList = getGoogleList(list.toList.head)
    //println("Finallist: " + finalList)
    list.foreach(word => {
      writer.append(word)
    })

    writer.close()
    //println("Formatted: " + readFromFile().flatMap(_.split(",")).toList)
  }

  def readFromFile(): ListBuffer[String] = {
    /*val resource = getClass.getResourceAsStream("/resources/output.txt")
    Source.fromInputStream(resource).getLines().to[ListBuffer]*/
    Source.fromFile("/Users/Phrida/IdeaProjects/Masterprosjekt/src/main/scala/resources/output.txt").getLines.to[ListBuffer]
  }





}



