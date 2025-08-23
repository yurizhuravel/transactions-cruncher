package cruncher

import cruncher.Domain._
import cruncher.DataIngestSetup._
import org.apache.spark.sql.{Dataset, Encoder, Encoders}
import org.apache.spark.sql.functions._

object AvgByTypeCalculator extends App {

  def avgTransactionsByType(sourceDS: Dataset[Transaction]): Dataset[AverageByTxType] = {
    val targetTxTypes = Seq("AA", "BB", "CC", "DD", "EE", "FF", "GG")
    logger.info(s"Starting average calculation for transaction types ${targetTxTypes.mkString(",")}")

    import spark.implicits._
    sourceDS
      .toDF()
      .groupBy("accountId")
      .pivot("category", targetTxTypes)
      .agg(round(avg("transactionAmount"), 2))
      .na.fill(0.0, targetTxTypes)
      .orderBy(regexp_extract(col("accountId"), "\\d+", 0).cast("int"))
      .as[AverageByTxType]
  }

  avgTransactionsByType(transactionsDS).show(100)

  spark.stop()
}