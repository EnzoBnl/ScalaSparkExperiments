package edu.stanford.nlp.simple
import java.util.Properties

import com.enzobnl.sparkscalaexpe.util.Utils
import edu.stanford.nlp.pipeline.Annotation
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations
import edu.stanford.nlp.simple.{Document, Sentence}
import edu.stanford.nlp.trees.{Tree, TreeCoreAnnotations}
import edu.stanford.nlp.trees.TreeCoreAnnotations.TreeAnnotation
import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage, Transformer}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.util.CoreMap

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/*
    import html2text
    import requests
    import os

    proc = html2text.HTML2Text()
    proc.ignore_links = True
    proc.ignore_images = True
    proc.emphasis_mark = ""
    proc.ul_item_mark = ""
    proc.strong_mark = ""

    def url_response_to_md(url):
        raw_text = requests.get(url).text
        return proc.handle(raw_text)
        # return list(filter(lambda e: len(e), md_text.replace("\n", " ").split(" ")))

    urls = [
        "https://www.oncrawl.com/seo-crawler/",
        "https://www.oncrawl.com/seo-log-analyzer/",
        "https://www.oncrawl.com/oncrawl-analytics/",
        "https://www.oncrawl.com/seo-crawl-logs-analysis/",
        "https://www.oncrawl.com/oncrawl-rankings/",
        "https://www.oncrawl.com/oncrawl-backlinks/",
        "https://www.oncrawl.com/oncrawl-platform/",
        "https://www.oncrawl.com/google-analytics-oncrawl/",
        "https://www.oncrawl.com/google-search-console-oncrawl/",
        "https://www.oncrawl.com/adobe-analytics-oncrawl/",
        "https://www.oncrawl.com/majestic-oncrawl/",
        "https://www.oncrawl.com/at-internet-oncrawl/",
        "https://toolbox.oncrawl.com/",
        "http://developer.oncrawl.com/"
    ]
    for url in urls:
        with open(url.replace("/", "_"), "w") as f:
            f.write(url_response_to_md(url))
     */
object KeyWordsExtraction extends Runnable {
  lazy val spark: SparkSession = {
    val spark =
      SparkSession
        .builder
        .config("spark.default.parallelism", "12")
        .master("local[*]")
        .appName("GraphxVsGraphFramesPageRanksApp")
        .getOrCreate
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

  lazy val stopWords: Set[String] = loadStopWords();

  def loadStopWords(): Set[String] = {
    scala.io.Source.fromURL("file:///home/enzo/Prog/scala-spark-experiments/src/main/resources/stopwords.txt").mkString.split("\n").toSet
  }

  def getExampleDataFrame1(completeTokenizer: String => Seq[String]): DataFrame = {
    val urls = Seq(
      "https://www.oncrawl.com/seo-crawler/",
      "https://www.oncrawl.com/seo-log-analyzer/",
      "https://www.oncrawl.com/oncrawl-analytics/",
      "https://www.oncrawl.com/seo-crawl-logs-analysis/",
      "https://www.oncrawl.com/oncrawl-rankings/",
      "https://www.oncrawl.com/oncrawl-backlinks/",
      "https://www.oncrawl.com/oncrawl-platform/",
      "https://www.oncrawl.com/google-analytics-oncrawl/",
      "https://www.oncrawl.com/google-search-console-oncrawl/",
      "https://www.oncrawl.com/adobe-analytics-oncrawl/",
      "https://www.oncrawl.com/majestic-oncrawl/",
      "https://www.oncrawl.com/at-internet-oncrawl/",
      "https://toolbox.oncrawl.com/",
      "http://developer.oncrawl.com/"
    )

    def fileContent(url: String): String = {
      val modifiedURL = url.replace("/", "_")
      Source.fromFile("./pages/" + modifiedURL).mkString
    }

    val contents = urls
      .map(url => (url, fileContent(url)))

    spark.createDataFrame(contents).toDF("url", "content")
      .withColumn("words", udf(completeTokenizer).apply(col("content")))
      .select("url", "words")
  }

  def baseTokenization(text: String): Seq[String] = {
      """[^a-zA-Z-_ ']+""".r.replaceAllIn(text.toLowerCase(), " ")
        .replace("\n", " ")
        .split(" ")
        .filter(_.size > 1)
  }

  def getTFIDFTransformer(df: DataFrame): PipelineModel = {
    // ML
    var stages: Array[PipelineStage] = Array()
    // TF IDF  TODO: adaptativenumFeatures for collision
    stages = stages :+ new HashingTF().setNumFeatures(2000000).setInputCol("words").setOutputCol("tf")
    stages = stages :+ new IDF() /*.setMinDocFreq()*/ .setInputCol("tf").setOutputCol("tfidf")

    val pipe = new Pipeline().setStages(stages)
    pipe.fit(df)
  }

  def isStopWord(word: String): Boolean = {
    stopWords.contains(word)
  }

  def getIndexToWordMapping(model: Transformer, df: DataFrame): Map[Long, String] = {
    // mapping
    import spark.implicits._
    model.transform(
      df
        .select("words")
        .withColumn("words", explode(col("words")))
        .distinct()
        .withColumn("words", array("words")
        )
    )
      .withColumn("indices",
        udf((v: org.apache.spark.ml.linalg.SparseVector) => v.indices(0)).apply(col("tfidf")
        )
      )
      .withColumn("words", col("words")(0))
      .select("indices", "words").as[(Long, String)].collect().toMap
  }

  def extractKeywords(model: Transformer, df: DataFrame, indexToWord: Map[Long, String], topX: Int): DataFrame = {
    import spark.implicits._
    model
      .transform(df)
      .map(page => {
        val url: String = page.getAs("url")
        val vector = page.getAs[org.apache.spark.ml.linalg.SparseVector]("tfidf")
        val threshhold = vector.values.sorted.takeRight(topX).min
        val keywords = vector.indices.foldLeft(List[(String, Double)]())((acc, index) => {
          val v = vector(index)
          if (v >= threshhold && acc.size < topX) acc :+ (indexToWord(index), v) else acc
        }
        )
        (url, keywords)
      }
      ).toDF("pages", "keywords")
  }

  def removeStopWords(df: DataFrame, colName: String): DataFrame = {
    df.withColumn(colName,
      udf((words: mutable.WrappedArray[String]) =>
        words.filter(!isStopWord(_))
      )
        .apply(col(colName)))
  }

  def runExtraction(completeTokenizer: String => Seq[String]) = {
    val df = removeStopWords(getExampleDataFrame1(completeTokenizer), "words")
    val tfidf: Transformer = getTFIDFTransformer(df)
    val indexToWord: Map[Long, String] = getIndexToWordMapping(tfidf, df)
    extractKeywords(tfidf, df, indexToWord, 3).show(false)
  }



  def stanfordCoreNLPTokenization(text: String): Seq[String] = {
    import scala.collection.JavaConversions._

    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma")
    val pipeline = new StanfordCoreNLP(props)

    val document = new Annotation(text)
    pipeline.annotate(document)
    val sentences: java.util.List[CoreMap] = document.get(classOf[CoreAnnotations.SentencesAnnotation])

    val words: ArrayBuffer[String] = ArrayBuffer[String]()
    for (sentence <- sentences) {
      for (token <- sentence.get(classOf[CoreAnnotations.TokensAnnotation])) {
        val lemma = token.get(classOf[CoreAnnotations.LemmaAnnotation])
        val pos = token.get(classOf[CoreAnnotations.PartOfSpeechAnnotation])
        if (!isStopWord(lemma)) words.append(s"[$pos]$lemma")
      }
    }
    words
  }

  override def run(): Unit = {
    Utils.time{runExtraction(baseTokenization)}
    Utils.time{runExtraction(stanfordCoreNLPTokenization)}
    // TODO: lemmatization
  }

  def lemmaWorkingExample() = {


    // 1. Set up a CoreNLP pipeline. This should be done once per type of annotation,// 1. Set up a CoreNLP pipeline. This should be done once per type of annotation,

    //    as it's fairly slow to initialize.
    // creates a StanfordCoreNLP object, with POS tagging, lemmatization, NER, parsing, and coreference resolution
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref")
    val pipeline = new StanfordCoreNLP(props)

    // 2. Run the pipeline on some text.
    // read some text in the text variable
    val text = "OnCrawl handles any advanced requirements, from lists of URLs, virtual robots.txt, DNS override, staging websites, subdomains or even URLs with parameters." // Add your text here!
    // create an empty Annotation just with the given text
    val document = new Annotation(text)
    // run all Annotators on this text
    pipeline.annotate(document)

    // 3. Read off the result
    // Get the list of sentences in the document
    val sentences: java.util.List[CoreMap] = document.get(classOf[CoreAnnotations.SentencesAnnotation])
    import scala.collection.JavaConversions._
    for (sentence <- sentences) { // traversing the words in the current sentence
      // a CoreLabel is a CoreMap with additional token-specific methods
      import scala.collection.JavaConversions._
      for (token <- sentence.get(classOf[CoreAnnotations.TokensAnnotation])) { // this is the text of the token
        val word = token.get(classOf[CoreAnnotations.TextAnnotation])
        // this is the POS tag of the token
        val pos = token.get(classOf[CoreAnnotations.PartOfSpeechAnnotation])
        // this is the NER label of the token
        val ne = token.get(classOf[CoreAnnotations.NamedEntityTagAnnotation])
        val lemma = token.get(classOf[CoreAnnotations.LemmaAnnotation])
        System.out.println("word: " + word + " pos: " + pos + " ne:" + ne + "lemma:" + lemma)
      }
      // this is the parse tree of the current sentence
      val tree = sentence.get(classOf[TreeCoreAnnotations.TreeAnnotation])
      System.out.println("parse tree:\n" + tree)
      // this is the Stanford dependency graph of the current sentence
      val dependencies = sentence.get(classOf[SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation])
      System.out.println("dependency graph:\n" + dependencies)
    }
  }
}

