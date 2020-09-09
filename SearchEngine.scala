// Databricks notebook source
import org.apache.spark.sql.functions._
import scala.math.{log, sqrt}
import org.apache.spark.{SparkConf, SparkContext}

// COMMAND ----------

val query = sc.textFile("/FileStore/tables/query.txt").collect.toList
val metadata = sc.textFile("FileStore/tables/movie_metadata.tsv")
val textfile = sc.textFile("FileStore/tables/plot_summaries.txt")
val stopwords = sc.textFile("FileStore/tables/stopwords.txt").flatMap(x => x.split("\n")).collect().toSet

// COMMAND ----------

val searchword= query(0)
val searchterm= query(1)

// COMMAND ----------

val movie = metadata.map(x => (x.split("\t")(0)->x.split("\t")(2)))
val text = textfile.map(x => x.replaceAll("""[\p{Punct}]""", " "))
val movieterm = text.map(x => x.toLowerCase.split("""\s+"""))
val output = movieterm.map(x => x.map(y => y).filter(word => stopwords.contains(word) == false))

// COMMAND ----------

val N = textfile.count

// COMMAND ----------

val search = searchword.toLowerCase().split(" ").map(x=>x.replaceAll("""[\p{Punct}]""", " ").trim().replaceAll("""( )+"""," "))

// COMMAND ----------

def tf(y: String)= output.map(x => (x(0), x.count(_.contains(y)).toDouble/x.size)).filter(x=>x._2!=0.0)

// COMMAND ----------

def df(x: String) = textfile.flatMap(y => y.split("\n").filter(t => t.contains(x))).map(y => ("t", 1)).reduceByKey(_ + _).collect()(0)._2

// COMMAND ----------

var TF = search.map(y => tf(y).collect().toMap)

// COMMAND ----------

var DF = search.map(y => df(y))

// COMMAND ----------

var IDF = DF.map(y => (1+ math.log(N/y+1)))

// COMMAND ----------

def tfidf(x: Int) = TF(x).map(a=>(a._1,a._2*IDF(x))).toMap

// COMMAND ----------

var TFIDF = TF.zipWithIndex.map{ case (e, i) =>tfidf(i) }

// COMMAND ----------

val sorted_TFIDF= TFIDF.flatten.sortBy(-_._2).toMap

// COMMAND ----------

val sortedTFIDF_RDD=sc.parallelize(sorted_TFIDF.toSeq)
movie.join(sortedTFIDF_RDD).map(x=>x._2._1).take(10)

// COMMAND ----------

val search = searchterm.toLowerCase().split(" ").map(x=>x.replaceAll("""[\p{Punct}]""", " ").trim().replaceAll("""( )+"""," "))

// COMMAND ----------

var TF = search.map(y => tf(y).collect().toMap)

// COMMAND ----------

var DF = search.map(y => df(y))

// COMMAND ----------

var IDF = DF.map(y => (1+ math.log(N/y+1)))

// COMMAND ----------

def tfidf(x: Int) = TF(x).map(a=>(a._1,a._2*IDF(x))).toMap

// COMMAND ----------

var TFIDF = TF.zipWithIndex.map{ case (e, i) =>tfidf(i) }

// COMMAND ----------

val TFquery =  search.map(y => search.count(_.contains(y)).toDouble/search.size)

// COMMAND ----------

val TF_IDFquery = TFquery.zipWithIndex.map{case (e, i) => e * IDF(i)}

// COMMAND ----------

val query = math.sqrt(TF_IDFquery.reduce((x,y) => x *x + y *y))
val distinct_id = TFIDF.flatMap(x => x.map(y=>y._1)).toList.distinct.toArray

// COMMAND ----------

def dot_function(x:String)= search.zipWithIndex.map{case (e, i) => (TF_IDFquery(i) * TFIDF(i).get(x).getOrElse(0.0).asInstanceOf[Double]).toDouble }.reduce((x,y)=>x+y)
val dotproduct = distinct_id.map(x =>  (x, dot_function(x))).toMap

// COMMAND ----------

def document_function(x:String)= search.zipWithIndex.map{case (e, i) => (TFIDF(i).get(x).getOrElse(0.0).asInstanceOf[Double]).toDouble }.reduce((x,y)=>x*x+y*y)
val document = distinct_id.map(x =>  (x, math.sqrt(document_function(x) ))).toMap

// COMMAND ----------

val cosine = distinct_id.map( x=> (x, dotproduct.get(x).getOrElse(0.0) /(document.get(x).getOrElse(0.0).asInstanceOf[Double] * query)))
val cosine_RDD= sc.parallelize(cosine)

// COMMAND ----------

movie.join(cosine_RDD).map(x=>(x._2._1,x._2._2)).sortBy(_._2).map(_._1).take(10)
