package pl.michalsz.spark

import org.apache.spark.sql.SparkSession

object CreateTable {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("pl.michalsz.spark.CreateTable")
      .getOrCreate()

    spark.sql("DROP TABLE IF EXISTS Accident")
    spark.sql("DROP TABLE IF EXISTS Time")
    spark.sql("DROP TABLE IF EXISTS Location")
    spark.sql("DROP TABLE IF EXISTS Surrounding")
    spark.sql("DROP TABLE IF EXISTS Temperature")
    spark.sql("DROP TABLE IF EXISTS Visibility")
    spark.sql("DROP TABLE IF EXISTS WeatherCondition")

    spark.sql(
      """
        |CREATE TABLE Accident (
        |   AccidentId BIGINT,
        |   Distance INT,
        |   NumberOfAccidents BINARY,
        |   Severity TINYINT,
        |   TimeId DATE,
        |   LocationId BIGINT,
        |   SurroundingId TINYINT,
        |   TemperatureId TINYINT,
        |   VisibilityId TINYINT,
        |   WeatherConditionId BIGINT
        |)
        |USING DELTA
        |LOCATION '/tmp/Accident'
        |""".stripMargin
    )

    spark.sql(
      """
        |CREATE TABLE Time (
        |   TimeId DATE,
        |   Year SHORT,
        |   Month TINYINT,
        |   Day TINYINT,
        |   DayOfWeek STRING
        |)
        |USING DELTA
        |LOCATION '/tmp/Time'
        |""".stripMargin
    )

    spark.sql(
      """
        |CREATE TABLE Location (
        |   LocationId BIGINT,
        |   Zipcode STRING,
        |   AirportCode STRING,
        |   City String,
        |   County STRING,
        |   State STRING,
        |   Country STRING,
        |   Street String
        |)
        |USING DELTA
        |LOCATION '/tmp/Location'
        |""".stripMargin
    )

    spark.sql(
      """
        |CREATE TABLE Surrounding (
        |   SurroundingId TINYINT,
        |   Crossing BOOLEAN,
        |   Railway BOOLEAN,
        |   Stop BOOLEAN
        |)
        |USING DELTA
        |LOCATION '/tmp/Surrounding'
        |""".stripMargin
    )

    spark.sql(
      """
        |CREATE TABLE Temperature (
        |   TemperatureId TINYINT,
        |   MinimumTemperature TINYINT,
        |   MaximumTemperature TINYINT,
        |   Description STRING
        |)
        |USING DELTA
        |LOCATION '/tmp/Temperature'
        |""".stripMargin
    )

    spark.sql(
      """
        |CREATE TABLE Visibility (
        |   VisibilityId TINYINT,
        |   MinimumDistance TINYINT,
        |   MaximumDistance TINYINT,
        |   Description STRING
        |)
        |USING DELTA
        |LOCATION '/tmp/Visibility'
        |""".stripMargin
    )

    spark.sql(
      """
        |CREATE TABLE WeatherCondition (
        |   WeatherConditionId BIGINT,
        |   Description STRING
        |)
        |USING DELTA
        |LOCATION '/tmp/WeatherCondition'
        |""".stripMargin
    )
  }
}
