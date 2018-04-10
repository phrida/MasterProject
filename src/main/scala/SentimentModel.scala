import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import utils.{PropertiesLoader, SQLContextSingleton}

object SentimentModel {

  val hashingTF = new HashingTF()

  def transformFeatures(tweetText:Seq[String]): linalg.Vector = {
    hashingTF.transform(tweetText)
  }

  def computeSentiment(text: String, stopWordsList: Broadcast[List[String]], model: NaiveBayesModel): Int = {
    val tweetInWords: Seq[String] = getStrippedTweetText(text, stopWordsList.value)
    val polarity = model.predict(transformFeatures(tweetInWords))
    normalizeMLlibSentiment(polarity)
  }

  def normalizeMLlibSentiment(sentiment: Double) = {
    sentiment match {
      case x if x == 0.0 => -1 //negative
      case x if x == 2.0 => 0 //neutral
      case x if x == 4.0 => 1 //positive
      case _ => 0 //neutral if model can't figure out sentiment
    }
  }

  def loadSentiment140File(sc: SparkContext, sentiment140FilePath: String): DataFrame = {
    val sqlContext = SQLContextSingleton.getInstance(sc)
    val tweetsDF = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .load(sentiment140FilePath)
      .toDF("polarity", "id", "date", "query", "user", "status")
    tweetsDF.drop("id").drop("date").drop("query").drop("user")
  }

  def createAndSaveNBModel(sc: SparkContext, stopWordsList: Broadcast[List[String]]): Unit = {
    val tweetsDF: DataFrame = loadSentiment140File(sc, PropertiesLoader.sentiment140TrainingSetFilePath)
    val labeledRDD = tweetsDF.select("polarity", "status").rdd.map {
      case Row(polarity: Int, tweet: String) =>
        val tweetInWords: Seq[String] = getStrippedTweetText(tweet, stopWordsList.value)
        LabeledPoint(polarity, transformFeatures(tweetInWords))
    }

    labeledRDD.cache()

    val naiveBayesModel: NaiveBayesModel = NaiveBayes.train(labeledRDD, lambda = 1.0, modelType = "multinomial")
    naiveBayesModel.save(sc, PropertiesLoader.naiveBayesModelPath)
  }

  def validateAccuracyOfNBModel(sc: SparkContext, stopWordsList: Broadcast[List[String]]): Unit = {
    val naiveBayesModel: NaiveBayesModel = NaiveBayesModel.load(sc, PropertiesLoader.naiveBayesModelPath)
    val tweetsDF: DataFrame = loadSentiment140File(sc, PropertiesLoader.sentiment140TestingFilePath)
    val actualVSpredictionRDD = tweetsDF.select("polarity", "status").rdd.map {
      case Row(polarity: Int, tweet: String) =>
        val tweetText = TweetGenerator.replaceNewLines(tweet)
        val tweetInWords: Seq[String] = getStrippedTweetText(tweetText, stopWordsList.value)
        (polarity.toDouble, naiveBayesModel.predict(transformFeatures(tweetInWords)), tweetText)
    }
    val accuracy = 100.0 * actualVSpredictionRDD.filter(x => x._1 == x._2).count() / tweetsDF.count()
    println(f"""\n\t<==******** Prediction accuracy compared to actual: $accuracy%.2f%% ********==>\n""")
    saveAccuracy(sc, actualVSpredictionRDD)
  }

  def saveAccuracy(sc: SparkContext, actualVSpredictionRDD: RDD[(Double, Double, String)]): Unit = {
    val sqlContext = SQLContextSingleton.getInstance(sc)
    import sqlContext.implicits._
    val actualVSpredictionDF = actualVSpredictionRDD.toDF("Actual", "Predicted", "Text")
    actualVSpredictionDF.coalesce(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .option("codec", classOf[GzipCodec].getCanonicalName)
      .mode(SaveMode.Append)
      .save(PropertiesLoader.modelAccuracyPath)
  }

  def getStrippedTweetText(tweetText: String, stopWordsList: List[String]): Seq[String] = {
    tweetText.toLowerCase
      .replaceAll("\n", "")
      .replaceAll("rt\\s+", "")
      .replaceAll("\\s+@\\w+", "")
      .replaceAll("@\\w+", "")
      .replaceAll("\\s+#\\w+", "")
      .replaceAll("#\\w+", "")
      .replaceAll("(?:https?|http?)://[\\w/%.-]+", "")
      .replaceAll("(?:https?|http?)://[\\w/%.-]+\\s+", "")
      .replaceAll("(?:https?|http?)//[\\w/%.-]+\\s+", "")
      .replaceAll("(?:https?|http?)//[\\w/%.-]+", "")
      .split("\\W+")
      .filter(_.matches("^[a-zA-Z]+$"))
      .filter(!stopWordsList.contains(_))

  }



}
