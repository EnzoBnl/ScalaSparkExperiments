package com.enzobnl.sparkscalaexpe.playground

import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage, Transformer}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

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

  def getExampleDataFrame1(): DataFrame = {
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
      .withColumn("words", udf(
        (content: String) =>
          """,?;.:/!*#"'{}()[]|\`@’""".map(c => (c.toString, ""))
          .foldLeft(content.toLowerCase())((content, pair) => content.replace(pair._1, pair._2))
          .replace("\n", " ")
            .split(" ")
            .filter(e => !e.isEmpty)
      ).apply(col("content")))
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

  def getKeyWords(model: Transformer, df: DataFrame, indexToWord: Map[Long, String]): DataFrame = {
    import spark.implicits._
    model
      .transform(df)
      .map(page => {
        val topX = 5
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

  override def run(): Unit = {
    val df = getExampleDataFrame1()
    val tfidf: Transformer = getTFIDFTransformer(df)
    val indexToWord: Map[Long, String] = getIndexToWordMapping(tfidf, df)
    getKeyWords(tfidf, df, indexToWord).show(false)


    // TODO: lemmatization
  }
}

