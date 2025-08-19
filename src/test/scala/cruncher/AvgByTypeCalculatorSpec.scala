package cruncher

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import cruncher.Domain._
import cruncher.AvgByTypeCalculator
import org.apache.spark.sql.Dataset

class AvgByTypeCalculatorSpec extends AnyFunSuite with Matchers {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("AvgByTypeCalculatorTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  test("avgTransactionsByType should calculate average transaction amounts by type correctly") {
 
    val transactions = Seq(
      Transaction("T1", "A1", 1, "AA", 100.0),
      Transaction("T2", "A1", 1, "BB", 200.0),
      Transaction("T3", "A1", 1, "AA", 300.0),
      Transaction("T4", "A2", 1, "CC", 400.0),
      Transaction("T5", "A2", 1, "DD", 500.0),
      Transaction("T6", "A2", 1, "CC", 600.0),
      Transaction("T7", "A3", 1, "EE", 700.0),
      Transaction("T8", "A3", 1, "FF", 800.0),
      Transaction("T9", "A3", 1, "GG", 900.0)
    ).toDS()

    val expected = Seq(
      AverageByTxType("A1", 200.0, 200.0, 0.0, 0.0, 0.0, 0.0, 0.0),
      AverageByTxType("A2", 0.0, 0.0, 500.0, 500.0, 0.0, 0.0, 0.0),
      AverageByTxType("A3", 0.0, 0.0, 0.0, 0.0, 700.0, 800.0, 900.0)
    ).toDS()

    val result: Dataset[AverageByTxType] = AvgByTypeCalculator.avgTransactionsByType(transactions)

    result.collect() should contain theSameElementsAs expected.collect()
  }
}