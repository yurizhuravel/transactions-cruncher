## Basic data handling with Scala and Spark

This is a sample Scala + Spark project to demonstrate ingesting data from a file and applying transformations to it.

### Usage

The service is built and run with `sbt`, which you can get [here](https://www.scala-sbt.org/).

After installing `sbt` and checking out this repository, you can run the example cases as follows from the root of the project:

1. Daily total calculation: `sbt run1`
2. Average by transaction type: `sbt run2`
3. Rolling window stats: `sbt run3`
