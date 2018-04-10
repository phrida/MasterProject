import java.io._
import java.text.SimpleDateFormat

import twitter4j._
import net.liftweb.json._
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.elasticsearch.spark.rdd.EsSpark

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scalaj.http.{Http, HttpResponse}
import utils.{PropertiesLoader, SQLContextSingleton}

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
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.sparkContext.setLogLevel("OFF")
    val stopWordsList = ssc.sparkContext.broadcast(loadStopWords())

    //Create Naive Bayes Model
    SentimentModel.createAndSaveNBModel(ssc.sparkContext, stopWordsList)
    //Validate Accuracy
    SentimentModel.validateAccuracyOfNBModel(ssc.sparkContext, stopWordsList)
    val naiveBayesModel = NaiveBayesModel.load(ssc.sparkContext, PropertiesLoader.naiveBayesModelPath)



    val initialFile = new ListBuffer[String]
    initialFile.append("Initial")
    writeToFile(initialFile)
    println("Initial file: " + readFromFile())

    def predictSentiment(status: Status): Int = {
      var returnedSentiment = 0
      val tweetText = replaceNewLines(status.getText)
      val mllibsentiment = {
        if (isTweetInEnglish(status)) {
          returnedSentiment = SentimentModel.computeSentiment(tweetText, stopWordsList, naiveBayesModel)
        } else {
          returnedSentiment = 0 //return neutral sentiment
        }
      }
      return returnedSentiment
    }

    def translateSentiment(sentiment: Int): String = {
      sentiment match {
        case x if x == -1 => "NEGATIVE" //negative
        case x if x == 0 => "NEUTRAL" //neutral
        case x if x == 1 => "POSITIVE" //positive
        case _ => "NEUTRAL" //neutral if model can't figure out sentiment
      }
    }

    val filterLines = KafkaUtils.createStream(ssc, zkQuorum, group, Map("keyworddata" -> 1)).map(_._2)
    val filterWords = filterLines.flatMap(_.split(" "))
    filterWords.print()

    filterWords.foreachRDD(rdd => {
      rdd.foreach(word => {
        val words = new ListBuffer[String]
        val googleWords = getGoogleList(word)
        println("Word to append: " + word)
        googleWords.foreach(word => words.append(word))
        println("Words in ListBuffer: " + words)
        writeToFile(words)
        println("Words in file: " + readFromFile())
        println("Head in file: " + readFromFile())
        val status = "this is a test status"
        val filter = List("this", "hej", "status")
        println("Test of function: " + filter.exists(status.contains(_)))
      })
    })

    val tweets = KafkaUtils.createStream(ssc, zkQuorum, group, Map("twitterdata" -> 1)).map(_._2)

    //tweets.print()
    //val tweets = TwitterUtils.createStream(ssc.get, None)

    //val filteredTweets = tweets.filter(_.contains(readFromFile()))
    val filteredTweets = tweets.filter(status => readFromFile().exists(status.contains(_)))

    val tweetMap = filteredTweets.map(json => {
      val status = TwitterObjectFactory.createStatus(json)
      val hashtags = status.getHashtagEntities().map(_.getText())
      val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
      val sentiment = predictSentiment(status)
      val translatedSentiment = translateSentiment(sentiment)


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
        "statusLanguage" -> status.getLang,
        "sentiment" -> translatedSentiment
      )
    })

    tweetMap.print()

    //tweetMap.foreachRDD(tweet => EsSpark.saveToEs(tweet, "visualization/tweets"))


    ssc.start()
    ssc.awaitTermination()

  }

  def getGoogleList(keyword: String): ListBuffer[String] = {
    //println("Keyword:" + keyword)
    val response: HttpResponse[String] = Http("https://kgsearch.googleapis.com/v1/entities:search")
      .params(Seq("query" -> keyword.toString, "limit" -> 10.toString, "key" -> "AIzaSyAR0679Of_1TcUWQhgQS-_7hYSSv3SnE8s"))
      .asString
    val json = parse(response.body)
    val listObject = json \\ "name"
    val list: List[String] = listObject \\ classOf[JString]
    list.to[ListBuffer]
  }

  def writeToFile(list: ListBuffer[String]): Any = {

    val file = new File("/Users/Phrida/IdeaProjects/Masterprosjekt/src/main/scala/resources/output.txt")
    println(file.exists())
    val writer = new PrintWriter(file)

    list.foreach(word => {
      writer.append(word + ",")
    })

    writer.close()
  }

  def readFromFile(): List[String] = {
    /*val resource = getClass.getResourceAsStream("/resources/output.txt")
    Source.fromInputStream(resource).getLines().to[ListBuffer]*/
    Source.fromFile("/Users/Phrida/IdeaProjects/Masterprosjekt/src/main/scala/resources/output.txt").getLines.to[List].flatMap(_.split(","))
  }

  def replaceNewLines(tweetText: String): String = {
    tweetText.replaceAll("\n", "")
  }


  def loadStopWords(): List[String] = {
    val resource = getClass.getResourceAsStream("/resources/StopWord_Corpus.txt")
    Source.fromInputStream(resource).getLines().toList

  }

  def isTweetInEnglish(status: Status): Boolean = {
    status.getLang == "en" && status.getUser.getLang == "en"
  }





}



