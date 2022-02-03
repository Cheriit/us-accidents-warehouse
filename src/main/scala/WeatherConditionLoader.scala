package pl.michalsz.spark

import model.WeatherCondition

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object WeatherConditionLoader {

  val weatherRegex =
    """^[a-zA-Z\d~\.,: %\-\(\)]*, Weather Condition: ([a-zA-Z ]*|~)$"""

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
                                          .appName("pl.michalsz.spark.WeatherConditionLoader")
                                          .getOrCreate()
    import spark.implicits._

    val filesLocation = args(0)
    val unparsedWeatherDF = spark.read
                                 .textFile(s"$filesLocation/weather*")
                                 .toDF("value")

    val weatherDS = unparsedWeatherDF
      .filter(col("value").rlike(weatherRegex))
      .select(
        functions.regexp_replace(col("value"), weatherRegex, "$1").as("description")
        )
      .distinct()
      .na.fill("Unknown")
      .withColumn("weatherConditionId", monotonically_increasing_id)
      .select("weatherConditionId", "description")
      .as[WeatherCondition]

    weatherDS.write
             .format("delta")
             .insertInto("WeatherCondition")
  }
}
