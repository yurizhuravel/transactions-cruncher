package cruncher

import cruncher.DataIngestSetup._
import cruncher.Domain._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.scalalang.typed
import scala.math.BigDecimal.RoundingMode

object DailyTotalCalculator extends App {

  def toTwoDecimals(num: Double) = BigDecimal(num).setScale(2, RoundingMode.HALF_UP).toDouble // rounding to money format

  def dailyTransactionsTotal(sourceDS: Dataset[Transaction]): Dataset[DailyTotal] = {
    import spark.implicits._
    
    sourceDS
    .groupByKey(_.transactionDay)
    .agg(typed.sum[Transaction](_.transactionAmount).name("dailyTransactionsTotal"))
    .map { case (day, total) =>        
      DailyTotal(day, toTwoDecimals(total))
    }
    .sort('transactionDay)
}

  dailyTransactionsTotal(transactionsDS).show(100)
  
  spark.stop()
}