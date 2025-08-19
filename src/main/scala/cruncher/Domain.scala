package cruncher

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoder, Encoders}

object Domain {

  val transactionsSchema = StructType(Array(
    StructField("transactionId", StringType),
    StructField("accountId", StringType),
    StructField("transactionDay", LongType),
    StructField("category", StringType),
    StructField("transactionAmount", DoubleType)
    )
  )
  
  final case class Transaction(
    transactionId: String,
    accountId: String,
    transactionDay: Long,
    category: String,
    transactionAmount: Double
  )

  final case class DailyTotal(
    transactionDay: Long,
    dailyTransactionsTotal: Double
  )

  final case class AverageByTxType(
    accountId: String,
    AA: Double,
    BB: Double,
    CC: Double,
    DD: Double,
    EE: Double,
    FF: Double,
    GG: Double
  )

  final case class DailyAggregated(
    accountId: String,
    transactionDay: Long,
    maxPerDay: Double,
    totalPerDay: Double,
    countPerDay: Int,
    AAPerDay: Double,
    CCPerDay: Double,
    FFPerDay: Double
  )

  final case class RollingStats(
    transactionDay: Long,
    accountId: String,
    maxTransaction: Double,
    avgTransaction: Double,
    AATotalValue: Double,
    CCTotalValue: Double,
    FFTotalValue: Double
  )
}
