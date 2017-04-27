// Databricks notebook source
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

object Ejem1SparkStreaming {
 def main(args: Array[String]): Unit = {
   val conf = new SparkConf().setMaster("local[4]").setAppName("Ejemplo1SparkStreaming")
   val ssc = new StreamingContext(conf, Seconds(2))
   val opcion = if (!args.isEmpty) args(0) else "0"
   opcion match {
     case "0" => ejemBasico(ssc) 
     case _ => println("-- Opcion incorrecta --")
   }
 }
 def ejemBasico(ssc: StreamingContext) {
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()
    ssc.start() 
    ssc.awaitTermination() 
 }
}
