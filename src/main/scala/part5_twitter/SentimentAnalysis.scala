package part5_twitter

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import edu.stanford.nlp.util.CoreMap

import java.util.Properties
import scala.collection.JavaConverters._

object SentimentAnalysis {

  def createNlpProps() = {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment") //nlp library properties
    props
  }

  def detectSentiment(message: String): SentimentType = {
    val pipeline = new StanfordCoreNLP(createNlpProps())
    val annotation = pipeline.process(message) // all the scores attached to this message

    // split the text into sentences and attach scores to each
    val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation]).asScala
    val sentiments = sentences.map { sentence: CoreMap =>
      val tree = sentence.get(classOf[SentimentCoreAnnotations.AnnotatedTree])
      // convert the score to a double for each sentence
      RNNCoreAnnotations.getPredictedClass(tree).toDouble
    }

    // average out all the sentiments detected in this text
    val avgSentiment =
      if (sentiments.isEmpty) -1 // Sentiment not understood
      else sentiments.sum / sentiments.length // average sentiment vs sentiment/sentiment length

    SentimentType.fromScore(avgSentiment)

  }
}
