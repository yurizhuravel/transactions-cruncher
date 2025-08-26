package cruncher

import org.apache.spark.sql.SparkSession

import cruncher.Domain._

object DataIngestSetup {
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("Transaction Analyzer")
    .master("local[*]")
    .getOrCreate()

  val transactionsFilePath = "src/main/resources/data"

  val transactionsDFOptions = Map(
    "inferSchema" -> "false",
    "header" -> "true",
    "sep" -> ",",
    "nullValue" -> "",
    "mode" -> "failFast"
  )

  val transactionsDF = spark.read
    .schema(transactionsSchema)
    .options(transactionsDFOptions)
    .csv(transactionsFilePath)

  val transactionsDS = {
    import spark.implicits._
    transactionsDF.as[Transaction]
  }
}