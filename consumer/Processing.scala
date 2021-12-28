package TwitterMovieSent

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random
import org.apache.spark.sql.SparkSession
import scala.math.BigInt
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Encoders
import com.databricks.spark.corenlp.functions._
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.streaming.OutputMode

import vegas._
import vegas.spec.Spec.MarkEnums.Rule
import vegas.render.WindowRenderer._
import vegas.sparkExt._

import java.net._
import java.io._
import scala.io._

object Consumer {
  def main(args: Array[String]) {

    val ss = SparkSession.builder.master("local[1]").config("spark.sql.codegen.wholeStage", "false").appName("sparkKafkaConsumer").getOrCreate()
    ss.conf.set("spark.sql.crossJoin.enabled", true)
    import ss.implicits._
    val sc = ss.sparkContext

    val movie_schema = new StructType()
      .add("tconst", StringType, false)
      .add("primaryTitle", StringType, false)
      .add("startYear", IntegerType, false)
    val movieInputDF = ss.read.options(Map("inferSchema" -> "true", "header" -> "true")).csv("../recent.csv")

    def movie_regex = udf({(title: String) =>
      var beforeTitle = "\\A.*[^0-9a-zA-Z]" // anything at least 0 times followed by non title char
      if (title.split(" ") == 1) { // title is one word. might be a common word that is simply capitalized at the start of the sentence.
        beforeTitle = "\\A([^0-9a-zA-Z]|.+[^0-9a-zA-Z])" // non title char OR anything at least 1 time followed by non title char
      }
      val afterTitle = "([^0-9a-zA-Z].*\\Z|\\Z)" // non title char followed by anthing at least 0 times OR the end of the string
      s"$beforeTitle\\Q$title\\E$afterTitle"
    })

    val movieDF = movieInputDF.select(col("primaryTitle"), movie_regex(col("primaryTitle")).as("regex"))
    movieDF.persist()
    val inputDF = ss.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "tweets").load()
    
    def rlike_movie = udf({ (tweet: String, movie_title_pattern: String) => 
      movie_title_pattern.r.findFirstIn(tweet) match {
        case Some(_) => true
        case _ => false
    }})

    val tweetDF = inputDF.select(col("value").cast(StringType).as("tweet"), col("timestamp")).as[(String, Long)]
    val joinDS = tweetDF.join(movieDF, lower(tweetDF("tweet")).contains(lower(movieDF("primaryTitle"))), "inner")
    val filteredDS = joinDS.where(rlike_movie(col("tweet"), col("regex")))
    val textDS = filteredDS.select(col("primaryTitle").as("title"), col("tweet").as("text"), col("timestamp"))
    val lineDS = textDS.select(col("title"), explode(ssplit(col("text"))).as("lines"), col("timestamp"))
    val sentimentDS = lineDS.select(col("title"), regexp_replace(col("lines"), "\n", "").as("lines"), (sentiment(col("lines")) - 2).as("sentiment"), col("timestamp"))

    val slidingMetric = sentimentDS.groupBy(window(col("timestamp"), "60 minute", "30 second"), col("title")).agg(
      avg("sentiment").alias("avg"),
      stddev_pop("sentiment").alias("std") // This is a biased std, maybe change
    )
    val plotDF = slidingMetric.select(col("window"), col("title"), col("avg"), (col("avg")+col("std")).as("+std"), (col("avg")-col("std")).as("-std"))
    val fileWriter = new BufferedWriter(new FileWriter("../index.html", false))
    plotDF.writeStream.foreachBatch( (batchDF: DataFrame, batchId: Long) => {
      val batchPlot = batchDF.orderBy("window").coalesce(1).dropDuplicates(Seq("title")).drop(col("window"))
      val plot = Vegas.layered("Box plot of movies").
        withDataFrame(batchPlot).
        configAxis(tickLabelFontSize=Some(20.0), titleFontSize=Some(40.0)).
        withLayers(
          Layer().
            mark(Point).
            configMark(filled=Some(true), orient=MarkOrient.Vertical, fontSize=Some(48.0)).
            encodeX("title", Nom, title="Movie title").
            encodeY("avg", Quant, title="Sentiment", scale=Scale(scaleType=vegas.spec.Spec.ScaleTypeEnums.Linear, domainValues=Some(List(-4.0, 4.0)))),
          Layer().
            mark(Rule).
            configMark(orient=MarkOrient.Vertical, fontSize=Some(48.0)).
            encodeX("title", Nom).
            encodeY("+std", Quant, scale=Scale(scaleType=vegas.spec.Spec.ScaleTypeEnums.Linear, domainValues=Some(List(-4.0, 4.0)))).
            encodeY2("-std", Quant)
            ).configCell(width=1000, height=400).configMark(fontSize=Some(48.0))
      fileWriter.write(plot.html.pageHTML())
      fileWriter.flush()
      batchPlot.show
    }).outputMode("complete").option("checkpointLocation", "checkpoints/").start().awaitTermination()

  }
}
