import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Creates a Model of the training dataset using Spark MLlib's Naive Bayes classifier.
  */
// spark-submit --class "org.p7h.spark.sentiment.mllib.SparkNaiveBayesModelCreator" --master spark://spark:7077 spark-streaming-corenp-mllib-tweet-sentiment-assembly-0.1.jar
object TwitterNaiveBayesModelCreator {

  def main(args: Array[String]) {
    val sc = createSparkContext()

    //LogUtils.setLogLevels(sc)

    val stopWordsList = sc.broadcast(StopwordsLoader.loadStopWords(PropertiesLoader.nltkStopWords))
    createAndSaveNBModel(sc, stopWordsList)
//    validateAccuracyOfNBModel(sc, stopWordsList)
  }

  /**
    * Remove new line characters.
    *
    * @param tweetText -- Complete text of a tweet.
    * @return String with new lines removed.
    */
  def replaceNewLines(tweetText: String): String = {
    tweetText.replaceAll("\n", "")
  }

  /**
    * Create SparkContext.
    * Future extension: enable checkpointing to HDFS [is it really reqd??].
    *
    * @return SparkContext
    */
  def createSparkContext(): SparkContext = {
   /* val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
    val sc = SparkContext.getOrCreate(conf)*/

    System.setProperty("hadoop.home.dir", "C:\\Users\\mraje\\Documents\\UMKC\\KDM\\winutils")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()
    val sc = spark.sqlContext.sparkContext
    sc
  }

  /**
    * Creates a Naive Bayes Model of Tweet and its Sentiment from the Sentiment140 file.
    *
    * @param sc            -- Spark Context.
    * @param stopWordsList -- Broadcast variable for list of stop words to be removed from the tweets.
    */
  def createAndSaveNBModel(sc: SparkContext, stopWordsList: Broadcast[List[String]]): Unit = {
    val tweetsDF: DataFrame = loadSentiment140File(sc, PropertiesLoader.sentiment140TrainingFilePath)

    //tweetsDF.show()

    tweetsDF.printSchema()

    val allDistinctData = tweetsDF.randomSplit(Array(.70,.30),13L)
    val trainingDataSet = allDistinctData(0)
    val testingDataSet = allDistinctData(1)

    val tw1= trainingDataSet.filter("_c1 is not null")
    //val labeledRDD = tweetsDF.select("score", "text").rdd.map {
      val labeledRDD = tw1.rdd.map {
      case Row(text: String, score: Int) =>
        println(text)
        //val scoreInt = score.toLong
        val scoreInt = score
        val tweetInWords: Seq[String] = MLlibSentimentAnalyzer.getBarebonesTweetText(text, stopWordsList.value)
        LabeledPoint(scoreInt, MLlibSentimentAnalyzer.transformFeatures(tweetInWords))
    }

     labeledRDD.randomSplit(Array(.70,.30),13L)


    val naiveBayesModel: NaiveBayesModel = NaiveBayes.train(labeledRDD, lambda = 1.0, modelType = "multinomial")
    //naiveBayesModel.save(sc, PropertiesLoader.naiveBayesModelPath)

    val naiveBayesModel : NaiveBayesModel = NaiveBayesModel.load(sc, "data/tweets_sentiment/NBModel")

    val tw2= testingDataSet.filter("_c1 is not null")
    val actualVsPredictionRDD = tw2.rdd.map {
      case Row(tweet: String, score: Int) =>
        val tweetText = replaceNewLines(tweet)
        val tweetInWords: Seq[String] = MLlibSentimentAnalyzer.getBarebonesTweetText(tweetText, stopWordsList.value)
        (score.toDouble,
          naiveBayesModel.predict(MLlibSentimentAnalyzer.transformFeatures(tweetInWords))
          )
    }


    val metrics = new MulticlassMetrics(actualVsPredictionRDD)

    val confusionMatrix = metrics.confusionMatrix
    println("Twitter Model Confusion Matrix= \n",confusionMatrix)


    println( "Twitter Model  Accuracy " + metrics.accuracy )

    val myModelStat=Seq(metrics.precision,metrics.fMeasure,metrics.recall)
    val accuracy = 100.0 * actualVsPredictionRDD.filter(x => x._1 == x._2).count() / tweetsDF.count()

    println("Accuracy = " + accuracy)

  }

  /**
    * Validates and check the accuracy of the model by comparing the polarity of a tweet from the dataset and compares it with the MLlib predicted polarity.
    *
    * @param sc            -- Spark Context.
    * @param stopWordsList -- Broadcast variable for list of stop words to be removed from the tweets.
    */
  def validateAccuracyOfNBModel(sc: SparkContext, stopWordsList: Broadcast[List[String]]): Unit = {
    val naiveBayesModel: NaiveBayesModel = NaiveBayesModel.load(sc, PropertiesLoader.naiveBayesModelPath)

    val tweetsDF: DataFrame = loadSentiment140File(sc, PropertiesLoader.sentiment140TestingFilePath)
    val actualVsPredictionRDD = tweetsDF.select("polarity", "status").rdd.map {
      case Row(polarity: Int, tweet: String) =>
        val tweetText = replaceNewLines(tweet)
        val tweetInWords: Seq[String] = MLlibSentimentAnalyzer.getBarebonesTweetText(tweetText, stopWordsList.value)
        (polarity.toDouble,
          naiveBayesModel.predict(MLlibSentimentAnalyzer.transformFeatures(tweetInWords)),
          tweetText)
    }
    val accuracy = 100.0 * actualVsPredictionRDD.filter(x => x._1 == x._2).count() / tweetsDF.count()
    /*actualVsPredictionRDD.cache()
    val predictedCorrect = actualVsPredictionRDD.filter(x => x._1 == x._2).count()
    val predictedInCorrect = actualVsPredictionRDD.filter(x => x._1 != x._2).count()
    val accuracy = 100.0 * predictedCorrect.toDouble / (predictedCorrect + predictedInCorrect).toDouble*/
    println(f"""\n\t<==******** Prediction accuracy compared to actual: $accuracy%.2f%% ********==>\n""")
    //saveAccuracy(sc, actualVsPredictionRDD)
  }

  /**
    * Loads the Sentiment140 file from the specified path using SparkContext.
    *
    * @param sc                   -- Spark Context.
    * @param sentiment140FilePath -- Absolute file path of Sentiment140.
    * @return -- Spark DataFrame of the Sentiment file with the tweet text and its polarity.
    */
  def loadSentiment140File(sc: SparkContext, sentiment140FilePath: String): DataFrame = {

    val sqlContext = SQLContextSingleton.getInstance(sc)
    val tweetsDF = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .load(sentiment140FilePath).toDF()
      //.toDF("score","text")
   // polarity", "id", "date", "query", "user", "status")

    // Drop the columns we are not interested in.
     //tweetsDF.drop("id_str").drop("place_type").drop("full_name").drop("extended_tweet").drop("re_tweet")
    tweetsDF
  }

  /**
    * Saves the accuracy computation of the ML library.
    * The columns are actual polarity as per the dataset, computed polarity with MLlib and the tweet text.
    *
    * @param sc                    -- Spark Context.
    * @param actualVsPredictionRDD -- RDD of polarity of a tweet in dataset and MLlib computed polarity.
    */
 /* def saveAccuracy(sc: SparkContext, actualVsPredictionRDD: RDD[(Double, Double, String)]): Unit = {
    val sqlContext = SQLContextSingleton.getInstance(sc)
    val actualVsPredictionDF = actualVsPredictionRDD.toDF("Actual", "Predicted", "Text")
    actualVsPredictionDF.coalesce(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", "\t")
      // Compression codec to compress while saving to file.
      .option("codec", classOf[GzipCodec].getCanonicalName)
      .mode(SaveMode.Append)
      .save(PropertiesLoader.modelAccuracyPath)
  }*/
}