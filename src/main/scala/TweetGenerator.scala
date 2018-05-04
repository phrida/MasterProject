import java.io._
import java.text.SimpleDateFormat

import twitter4j._
import net.liftweb.json._
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Milliseconds, Minutes, Seconds, StreamingContext}
import org.elasticsearch.spark.rdd.EsSpark

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scalaj.http.{Http, HttpResponse}
import twitter4j.conf.ConfigurationBuilder
import utils.{PropertiesLoader, SQLContextSingleton}

import scala.collection.immutable.HashMap
import scala.io.Source



object TweetGenerator extends Serializable {

  var counter = 0

  def main(args: Array[String]): Unit = {

    val classification = new Classifier


    //Inställningar för twitter
    System.setProperty("twitter4j.oauth.consumerKey", "WeJAx0QjHyZuFIuOT0mCAlJqR")
    System.setProperty("twitter4j.oauth.consumerSecret", "AF1PYLqk6XPrgFMlYgDQq3l91v6eHpnimlH1u45OSX3yTggMvP")
    System.setProperty("twitter4j.oauth.accessToken", "384519993-b2PNRU3TiLxt5gTSUOlUamac7UuHZvWiF2pk9ZqU")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "xRnVgh2CpD41GKi0W0B2Z5JA6S8JRIL83W8NuuiuK4CtW")



    val zkQuorum = "localhost:2181"
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
    val group = "my-group"
    val twitterTopic = Set("twitterdata")
    val numThreads = 1
    //val Array(zkQuorum, group, topics, numThreads) = args;
    val sparkConf = new SparkConf().setAppName("TweetGenerator")//.setMaster("local[4]")
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
    //SentimentModel.createAndSaveNBModel(ssc.sparkContext, stopWordsList)
    //Validate Accuracy
    //SentimentModel.validateAccuracyOfNBModel(ssc.sparkContext, stopWordsList)
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

    val filterLines = KafkaUtils.createStream(ssc, zkQuorum, group, Map("keyworddata" -> 1)).map(_._2)
    val filterWords = filterLines.flatMap(_.split(" "))
    filterWords.print()

    filterWords.foreachRDD(rdd => {
      rdd.foreach(word => {
        val words = new ListBuffer[String]
        words.append(word)
        val googleWords = getGoogleList(word)
        println("Word to append: " + word)
        googleWords.foreach(word => words.append(word))
        writeToFile(words)
        println("Words in file: " + readFromFile())
        println("Counter: " + counter)
      })
    })

    classification.initialize()

    //val tweets = KafkaUtils.createStream(ssc, zkQuorum, group, Map("twitterdata" -> 1)).map(_._2)
    val tweets = ssc.socketTextStream("localhost", 10001)
    //tweets.foreachRDD(rdd => classification.classify(rdd.count()))

    val input = ssc.sparkContext.textFile("/home/spnorrha/IdeaProjects/MasterProject/src/main/scala/resources/sample.txt")
      .map(line => line.split(" ").toSeq)

    /*
    val word2vec = new Word2Vec
    val model = word2vec.fit(input)

    val synonyms = model.findSynonyms("1", 5)
    for ((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym, $cosineSimilarity")

    }*/




    //val filters = "Trump"


    //val filteredTweets = tweets.filter(_.contains(readFromFile()))

    val filteredTweets = tweets.filter(status => readFromFile().exists(status.contains(_)))

    //val filteredTweets = tweets.filter(_.contains(filters))



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
        "sentiment" -> predictSentiment(status),
        "deviceType" -> status.getSource
      )
    })


    tweetMap.foreachRDD(rdd => classification.classify(rdd.count()))

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

    val file = new File("/home/spnorrha/IdeaProjects/MasterProject/src/main/scala/resources/output.txt")
    println(file.exists())
    val writer = new FileWriter(file, true)

    list.foreach(word => {
      counter += 1
      writer.write(word + ",")
    })

    writer.close()
  }

  def readFromFile(): List[String] = {
    /*val resource = getClass.getResourceAsStream("/resources/output.txt")
    Source.fromInputStream(resource).getLines().to[ListBuffer]*/
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



