package cruncher

import org.apache.spark.sql._

trait SparkTester {
  val sparkTestSession: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("test spark app")
    .getOrCreate()
}