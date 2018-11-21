package src.main.scala

import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.trees.Tree
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations

object SentimentAnalyzer {


  def main(args: Array[String]): Unit = {

    /*TweetWithSentiment tweetWithSentiment = sentimentAnalyzer
                    .findSentiment("click here for your Sachin Tendulkar personalized digital autograph.");*/

    val svalue = findSentiment("is excited for the future and we are even more excited to expand our #Uncarrier strategy with @Sprint as New T-Mobile. We continued to dominate this quarter .")
    System.out.println("Tweet sentiment analysis " + svalue)
    val svalue1 = findSentiment("I can’t stand not being able to hear my girlfriend over the phone because of service. I want to hear her voice because that’s all I have of her now.")
    System.out.println("Tweet sentiment analysis " + svalue1)
    val svalue2 = findSentiment("Rima is angry")

    System.out.println("Tweet sentiment analysis " + svalue2)
  }

  def findSentiment(line: String): Int = { //public TweetWithSentiment findSentiment(String line) {
    val props = new Properties
    props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
    val pipeline = new StanfordCoreNLP(props)
    var mainSentiment = 0
    if (line != null && line.length > 0) {
      var longest = 0
      val annotation = pipeline.process(line)
      import scala.collection.JavaConversions._
      for (sentence <- annotation.get(classOf[CoreAnnotations.SentencesAnnotation])) { //Tree tree = sentence.get(SentimentCoreAnnotations.AnnotatedTree.class);
        val tree = sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])
        val sentiment = RNNCoreAnnotations.getPredictedClass(tree)
        val partText = sentence.toString
        if (partText.length > longest) {
          mainSentiment = sentiment
          longest = partText.length
        }
      }
    }

    /*if (mainSentiment == 2 || mainSentiment > 4 || mainSentiment < 0) {
                return null;

            }*/
    //TweetWithSentiment tweetWithSentiment = new TweetWithSentiment(line, toCss(mainSentiment));
    mainSentiment
  }


  private def toCss(sentiment: Int) = sentiment match {
    case 0 =>
      "alert alert-danger"
    case 1 =>
      "alert alert-danger"
    case 2 =>
      "alert alert-warning"
    case 3 =>
      "alert alert-success"
    case 4 =>
      "alert alert-success"
    case _ =>
      ""
  }



}
