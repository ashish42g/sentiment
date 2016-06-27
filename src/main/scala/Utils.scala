import java.io.FileNotFoundException
import java.util.Properties

import org.apache.spark.mllib.feature.HashingTF

import scala.collection.JavaConversions._

import edu.stanford.nlp.ling.CoreAnnotations.LemmaAnnotation
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation
import edu.stanford.nlp.pipeline.{StanfordCoreNLP, Annotation}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

object Utils {

  val tf = new HashingTF

  val stopWords = loadStopWords("StopWords.txt")
  val propHandle = loadFilterKeywords("App.properties")
  val props = new Properties()

  props.put("annotators", "tokenize, ssplit, pos, lemma")
  val pipeline = new StanfordCoreNLP(props)

  val nGram = toIntValue(propHandle.getProperty("ngram")).getOrElse(2)

  val filterKeyWords = {
    propHandle.getProperty("keyword").split(",").toList.map(_.trim).toArray
  }

  def loadStopWords(path: String) = {
    val stream = getClass.getResourceAsStream(path)
    val lines = scala.io.Source.fromInputStream(stream).getLines
    lines toSet
  }

  def toIntValue(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: Exception => None
    }
  }

  def loadFilterKeywords(filename: String) = {
    val filterKeyWords = Option(getClass.getClassLoader.getResourceAsStream(filename))
    filterKeyWords match {
      case Some(file) =>
        val prop = new Properties()
        prop.load(file)
        file.close()
        prop
      case None => throw new FileNotFoundException(s"Property File: $filename is not found")
    }
  }

  def parseFile(file: RDD[(String)]) = {
    //Replaced quotes (") with /t
    val inputData = file.map(_.replaceAll("(^\"|\"$)|(\",\")", "\t"))
    inputData.map(x => x.split("\t")).map(l => (l(1).trim, l(6).trim.toLowerCase))
  }

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    println("\n\nElapsed time: " + (t1 - t0) / 1000 + "ms")
    result
  }

  /** This method generates n-grams */
  def generateNGram(line: List[String], nGram: Int) = {

    def club = for {
      i <- 0 to line.size if i <= nGram
      j <- 2 to nGram if i + j <= line.size
      elem = line.slice(i, i + j).mkString(" ")
    } yield elem

    line ++ club
  }

  def removeLinksAndChars(tweet: String) = {
    val tags = "[#@\\(\\)\\-,_.:;]" r
    val urlPattern = "(https?:\\/\\/)?([\\da-z\\.-]+)\\.([a-z\\.]{2,6})([\\/\\w\\.-]*)*\\/?" r
    val emailPattern = "([a-z0-9_\\.-]+)@([\\da-z\\.-]+)\\.([a-z\\.]{2,6})" r

    val x = urlPattern replaceAllIn(tweet, "")
    val y = emailPattern replaceAllIn(x, "")

    tags replaceAllIn(y, "")
  }

  def cleanseData(tweet: String) = {
    val z = removeLinksAndChars(tweet)
    val filteredTweets = z.split("[^\\w\']+") toList

    val finalList = filteredTweets.filter(x => !(stopWords.contains(x) || x == "" || x.isEmpty))
    generateNGram(finalList, nGram)
  }

  def cleanData(tweet: String) = {
    val z = removeLinksAndChars(tweet)
    if (!z.isEmpty && z != "") Some(z.trim) else None
  }

  def plainTextToLemmas(text: String) = {
    val doc = new Annotation(text)
    pipeline.annotate(doc)
    val lemmas = new ArrayBuffer[String]()
    val sentences = doc.get(classOf[SentencesAnnotation])
    for (sentence <- sentences; token <- sentence.get(classOf[TokensAnnotation])) {
      val lemma = token.get(classOf[LemmaAnnotation])
      if (lemma.length > 0 && !stopWords.contains(lemma)) {
        lemmas += lemma.toLowerCase
      }
    }
    lemmas.mkString(" ")
  }

  def generateOpinionList(documents: RDD[String]) = {
    val wds = documents.flatMap(x => generateNGram(x.split("\\W+").toList, nGram)).filter(x => !x.isEmpty).map((_, 1)).reduceByKey(_ + _).map(x => x.swap).sortByKey(false).map(y => y.swap)
    wds.sortBy(x => (x._1.split(" ").size, x._2), false)
  }
}