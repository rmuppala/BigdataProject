
package src.main.scala

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import src.main.scala.SentimentAnalyzer


object JsonExtract {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\Users\\mraje\\Documents\\UMKC\\KDM\\winutils")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()
    val sc = spark.sqlContext.sparkContext

    val tweets = spark.sqlContext.jsonFile("data/sprint.json")

    tweets.createOrReplaceTempView("tweet")
    val extracted_tweets = spark.sql("select id_str, place.place_type,place.full_name, text , extended_tweet.full_text , retweeted_status.text as retext " +
      "from tweet")
    //extracted_tweets.show(10)
    extracted_tweets.createOrReplaceTempView("extweet")
    val tw1 = extracted_tweets

    val score=  extracted_tweets.collect().map( f=> {
      val text  = f.getAs("text").toString
      val id = f.getAs("id_str").toString
      var s = SentimentAnalyzer.findSentiment( text)
      if (s < 2 ) s = 1
      if (s > 2) s = 3
      if (s == null) s= 2
      (text,s)
      /*var sdf = spark.sqlContext.createDataFrame(Seq(
        (id,s)
      )).toDF("id_str", "score")*/

    }   ).toList

    import spark.implicits._
    val scoreDF = score.toDF("text", "score")
    scoreDF.createOrReplaceTempView("twscore")
    val twscore = spark.sql("select  extweet.id_str, twscore.score, place_type, full_name, twscore.text, full_text, retext from twscore ,extweet where twscore.text = extweet.text ")
    twscore.write.format("csv").save("data/sprintscore.txt")










  }

}