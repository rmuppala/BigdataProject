import com.datastax.spark.connector._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.clulab.processors.corenlp._

object TwitterStream {
  def main(args: Array[String]) {

    //Set the hadoop home directory and location of winutils
    //Adjust this as needed for your system
    System.setProperty("hadoop.home.dir", "E:\\winutils")

    // Create the context with a connection to cassandra
    val conf = new SparkConf()
      .set("cassandra.connection.host","127.0.0.1:9042")
      .setMaster("local[2]")
      .setAppName("twitterinformatics")
    val ssc = new StreamingContext(conf, Seconds(5))

    //The Cassandra table should be configured as follows.
    //CREATE KEYSPACE "tweets"
    //WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
    //
    //USE tweets;
    //CREATE TABLE tweet(
    //t_id bigint,
    //t_avgsent int,
    //t_bayes int,
    //t_geo text,
    //t_lang text,
    //t_location text,
    //t_text text.
    //PRIMARY KEY (t_id));
    //
    // t_id | t_avgsent | t_bayes | t_geo | t_lang | t_location | t_text
    //------+-----------+---------+-------+--------+------------+--------

    //Grab the arguments to use as Tweet text filters, set these in the run configuration in the IDE
    val filters = args.take(args.length)

    //Create a reference to the Tweet Stream, ensure to place your API credentials in a file named
    //twitter4j.proerties with the format:
    //debug=true
    //oauth.consumerKey=<ConsumerKey>
    //oauth.consumerSecret=<ConsumerSecret>
    //oauth.accessToken=<AccessToken>
    //oauth.accessTokenSecret=<AccessTokenSecret>
    val stream = TwitterUtils.createStream(ssc, None, filters)

    //Perform data analysis on the stream and format the contained RDDs to be in the correct column order for Cassandra
    val data = stream.map{status =>
      val sentiment = CoreNLPSentimentAnalyzer.sentiment(status.getText)
      val avgSent = sentiment.reduce(_+_)/sentiment.length
      
      //Replace 2 below with result from Bayes
      (status.getId, avgSent, 2, status.getGeoLocation, status.getLang, status.getUser.getLocation, status.getText)
    }

    //Save each element of the stream to Cassandra
    data.foreachRDD{rdd=>
      if (rdd.count() > 0) {
        rdd.saveToCassandra("tweets","tweet")
      }
    }

    //data.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
