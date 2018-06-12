import java.io._
import java.text.SimpleDateFormat

import twitter4j._
import net.liftweb.json._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.mllib.classification.{NaiveBayesModel}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.{SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.rdd.EsSpark

import scala.collection.mutable.{ListBuffer}
import scalaj.http.{Http, HttpResponse}
import utils.{PropertiesLoader}

import scala.collection.immutable.HashMap
import scala.io.Source



object TweetGenerator {

  val classification = new Classifier

  def main(args: Array[String]): Unit = {


    //Settings for Twitter4j
    System.setProperty("twitter4j.oauth.consumerKey", "WeJAx0QjHyZuFIuOT0mCAlJqR")
    System.setProperty("twitter4j.oauth.consumerSecret", "AF1PYLqk6XPrgFMlYgDQq3l91v6eHpnimlH1u45OSX3yTggMvP")
    System.setProperty("twitter4j.oauth.accessToken", "384519993-b2PNRU3TiLxt5gTSUOlUamac7UuHZvWiF2pk9ZqU")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "xRnVgh2CpD41GKi0W0B2Z5JA6S8JRIL83W8NuuiuK4CtW")


    //Setting for Kafka
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "my-group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val twitterTopic = Array("twitterdata")
    val keywordTopic = Array("keyworddata")


    //Settings for Spark and Elasticsearch
    val sparkConf = new SparkConf().setAppName("TweetGenerator")
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.resource", "visualization/tweets")
    sparkConf.set("es.nodes", "localhost")
    sparkConf.set("es.port", "9200")
    sparkConf.set("es.nodes.discovery", "true")
    sparkConf.set("es.net.http.auth.user", "elastic")
    sparkConf.set("es.net.http.auth.pass", "Dbn5RwvUltlomlot2pnS")
    val index_name = "twitter"
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.sparkContext.setLogLevel("OFF")


    val stopWordsList = ssc.sparkContext.broadcast(loadStopWords())

    //Create Naive Bayes Model
    SentimentModel.createAndSaveNBModel(ssc.sparkContext, stopWordsList)
    //Validate Accuracy
    SentimentModel.validateAccuracyOfNBModel(ssc.sparkContext, stopWordsList)
    val naiveBayesModel = NaiveBayesModel.load(ssc.sparkContext, PropertiesLoader.naiveBayesModelPath)



    def predictSentiment(status: Status): String = {
      var returnedSentiment = 0
      val tweetText = replaceNewLines(status.getText)
      val mllibsentiment = {
        if (isTweetInEnglish(status)) {
          returnedSentiment = SentimentModel.computeSentiment(tweetText, stopWordsList, naiveBayesModel)
        } else {
          returnedSentiment = 0 //return neutral sentiment
        }
      }
      return translateSentiment(returnedSentiment)
    }

    def translateSentiment(sentiment: Int): String = {
      sentiment match {
        case x if x == -1 => "NEGATIVE" //negative
        case x if x == 0 => "NEUTRAL" //neutral
        case x if x == 1 => "POSITIVE" //positive
        case _ => "NEUTRAL" //neutral if model can't figure out sentiment
      }
    }


    val filterLines = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](twitterTopic, kafkaParams)).map(record => (record.value))
    val filterWords = filterLines.flatMap(_.split(" "))

    filterWords.foreachRDD(rdd => {
      rdd.foreach(word => {
        val words = new ListBuffer[String]
        words.append(word)
        val googleWords = getGoogleList(word)
        googleWords.foreach(word => words.append(word))
        writeToFile(words)
      })
    })



    val tweets = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](keywordTopic, kafkaParams)).map(record => (record.value))
    classification.initialize()

    //Filter Tweets
    val filteredTweets = tweets.filter(status => readFromFile().exists(status.contains(_)))

    val tweetMap = filteredTweets.map(json => {
      val status = TwitterObjectFactory.createStatus(json)
      val hashtags = status.getHashtagEntities().map(_.getText())
      val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
      val sentiment = predictSentiment(status)


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
        "sentiment" -> sentiment,
        "deviceType" -> getStrippedDeviceType(status.getSource),
        "retweetCount" -> status.getRetweetCount
      )
    })

    tweetMap.foreachRDD(tweet => EsSpark.saveToEs(tweet, "visualization/tweets"))

    ssc.start()
    ssc.awaitTermination()

  }

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    val time = (t1 - t0) / 1000000.0
    classification.setTime(time)
    result
  }

  def getStrippedDeviceType(tweetText: String): String = {
    tweetText
      .replaceAll("\n", "")
      .replaceAll("rt\\s+", "")
      .replaceAll("\\s+@\\w+", "")
      .replaceAll("@\\w+", "")
      .replaceAll("\\s+#\\w+", "")
      .replaceAll("#\\w+", "")
      .replaceAll("(?:<a href=\")", "")
      .replaceAll("(?:https?|http?)://[\\w/%.-]+", "")
      .replaceAll("(?:https?|http?)://[\\w/%.-]+\\s+", "")
      .replaceAll("(?:https?|http?)//[\\w/%.-]+\\s+", "")
      .replaceAll("(?:https?|http?)//[\\w/%.-]+", "")
      .replaceAll("</a>", "")
      .replaceAll("\" rel=\"nofollow\">", "")
      .replaceAll("#!/download/ipad", "")
  }



  def getGoogleList(keyword: String): ListBuffer[String] = {
    val response: HttpResponse[String] = Http("https://kgsearch.googleapis.com/v1/entities:search")
      .params(Seq("query" -> keyword.toString, "limit" -> 9.toString, "key" -> "AIzaSyAR0679Of_1TcUWQhgQS-_7hYSSv3SnE8s"))
      .asString
    val json = parse(response.body.replaceAll(",", ""))
    val listObject = json \\ "name"
    val list: List[String] = listObject \\ classOf[JString]
    list.to[ListBuffer]
  }


  def writeToFile(list: ListBuffer[String]): Any = {
    val file = new File("/home/spnorrha/IdeaProjects/MasterProject/src/main/scala/resources/output.txt")
    val writer = new FileWriter(file, true)
    list.foreach(word => {
      writer.write(word + ",")
    })
    writer.close()
  }


  def readFromFile(): List[String] = {
    Source.fromFile("/home/spnorrha/IdeaProjects/MasterProject/src/main/scala/resources/output.txt").getLines.to[List].flatMap(_.split(","))
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



