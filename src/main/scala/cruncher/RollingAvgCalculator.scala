package cruncher

import cruncher.Domain._
import cruncher.DataIngestSetup._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object RollingAvgCalculator extends App {
  /*
  Note: Steps 2 and 3 are only necessary to strictly satisfy the 'one row per day per account' condition.
  These joins can be avoided if we only want to show days with non-null stats for all accounts.
  Also, to satisfy this condition, we are not dropping rows with only null transactions in the final step  
  */

  // 1: Pre-aggregate to one row per account per day and add count to use for averages later
  val dailyAggregatedDF = transactionsDS
    .toDF()
    .groupBy("accountId", "transactionDay")
    .agg(
      max("transactionAmount").as("maxPerDay"),
      sum("transactionAmount").as("totalPerDay"),
      count("*").as("countPerDay"),
      sum(when(col("category") === "AA", col("transactionAmount")).otherwise(0.0)).as("AAPerDay"),
      sum(when(col("category") === "CC", col("transactionAmount")).otherwise(0.0)).as("CCPerDay"),
      sum(when(col("category") === "FF", col("transactionAmount")).otherwise(0.0)).as("FFPerDay")
    )

  // 2: Create a grid of account-day combinations
  val allAccounts = transactionsDS.select("accountId").distinct()
  val allDays = transactionsDS.select("transactionDay").distinct()
  val allAccountDays = allAccounts.crossJoin(allDays)

  // 3: Join to ensure all account-day combinations exist, fill missing values with 0
  val maybeEmptyFields = Seq("maxPerDay", "totalPerDay", "countPerDay", "AAPerDay", "CCPerDay", "FFPerDay")
  val completeAccountDaysDF = allAccountDays
    .join(dailyAggregatedDF, Seq("accountId", "transactionDay"), "left")
    .na.fill(0.0, maybeEmptyFields)

  // 4: Set a window for previous 5 days excluding current day and calculate rolling statistics
  val fiveDayWindow = Window
    .partitionBy("accountId")
    .orderBy("transactionDay")
    .rangeBetween(-5, -1)

  val rollingStatsDF = completeAccountDaysDF
    .withColumn("maxTransaction", max("maxPerDay").over(fiveDayWindow))
    .withColumn("totalAmount", sum("totalPerDay").over(fiveDayWindow))
    .withColumn("totalCount", sum("countPerDay").over(fiveDayWindow))
    .withColumn("avgTransaction", 
      when(col("totalCount") > 0, round(col("totalAmount") / col("totalCount"), 2))
      .otherwise(0.0)
    )
    .withColumn("AATotalValue", round(sum("AAPerDay").over(fiveDayWindow), 2))
    .withColumn("CCTotalValue", round(sum("CCPerDay").over(fiveDayWindow), 2))
    .withColumn("FFTotalValue", round(sum("FFPerDay").over(fiveDayWindow), 2))
    .select(
      col("transactionDay"),
      col("accountId"),
      col("maxTransaction"),
      col("avgTransaction"),
      col("AATotalValue"),
      col("CCTotalValue"),
      col("FFTotalValue")
    )

  val orderedResult = {
    import spark.implicits._
    
    rollingStatsDF
      .filter(col("transactionDay") =!= 1)
      .withColumn("accountIdNum", regexp_extract(col("accountId"), "\\d+", 0).cast("int"))
      .orderBy("transactionDay", "accountIdNum")
      .drop("accountIdNum")
      .as[RollingStats]
  }
  
  orderedResult.show(2000)

  spark.stop()
}