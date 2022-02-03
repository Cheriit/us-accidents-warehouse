package pl.michalsz.spark

import model.{FileAccident, Time}

import org.apache.spark.sql.functions.{date_format, max, min, to_date}
import org.apache.spark.sql.{Encoders, SparkSession}

import java.time.LocalDate

object TimeLoader {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
                                          .appName("pl.michalsz.spark.TimeLoader")
                                          .getOrCreate()
    import spark.implicits._

    val filesLocation = args(0)
    val fileAccidentsDS = spark.read
                               .option("header", value = true)
                               .option("quote", "\"")
                               .option("header", "true")
                               .schema(Encoders.product[FileAccident].schema)
                               .csv(s"$filesLocation/main*")
                               .as[FileAccident]

    val flattenTimeDS = fileAccidentsDS
      .agg(min(to_date(fileAccidentsDS("startTime"))),
           max(to_date(fileAccidentsDS("endTime"))))
      .flatMap(row => row.getDate(0)
                         .toLocalDate
                         .toEpochDay
                         .to(row.getDate(1).toLocalDate.toEpochDay)
                         .map(LocalDate.ofEpochDay)
                         .toList
               )
    val timeDS = flattenTimeDS
      .select(
        flattenTimeDS("value").as("TimeId"),
        date_format(flattenTimeDS("value"), "y").cast("Short").as("year"),
        date_format(flattenTimeDS("value"), "M").cast("Short").as("month"),
        date_format(flattenTimeDS("value"), "d").cast("Short").as("day"),
        date_format(flattenTimeDS("value"), "E").as("dayOfWeek"),
        )
      .as[Time]


    timeDS.write
          .format("delta")
          .insertInto("Time")
  }
}
