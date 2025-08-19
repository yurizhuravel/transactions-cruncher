package cruncher

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import cruncher.Domain._
import cruncher.DailyTotalCalculator
import org.apache.spark.sql.Dataset

class DailyTotalCalculatorSpec extends AnyFunSuite with Matchers {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("Daily Total Test")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  test("dailyTransactionsTotal should calculate daily totals correctly") {

    val transactions = Seq(
      Transaction("T0001", "A1", 1, "AA", 100.0),
      Transaction("T0002", "A2", 1, "BB", 200.0),
      Transaction("T0003", "A3", 2, "CC", 300.0),
      Transaction("T0004", "A4", 2, "DD", 400.0),
      Transaction("T0005", "A5", 3, "EE", 500.0)
    ).toDS()

    val expected = Seq(
      DailyTotal(1, 300.0),
      DailyTotal(2, 700.0),
      DailyTotal(3, 500.0)
    ).toDS()

    val result: Dataset[DailyTotal] = DailyTotalCalculator.dailyTransactionsTotal(transactions)

    result.collect() should contain theSameElementsAs expected.collect()
  }
}