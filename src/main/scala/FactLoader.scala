package pl.michalsz.spark

import model._

import com.swoop.alchemy.spark.expressions.hll.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalDateTime}


object FactLoader {

  val temperatureUnknownId = 5
  val visibilityUnknownId = 5
  val weatherRegex =
    """^On (\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2})\.\d at the weather station at the airport ([A-Z\d]{4}) the following weather conditions were noted: Temperature \(F\): (-?\d{1,3}\.\d|~), Wind Chill \(F\): [-\d~\.]{1,6}, Humidity \(%\): [-\d~\.]{1,6}, Pressure \(in\): [-\d~\.]{1,7}, Visibility \(miles\): (\d{1,3}\.\d|~), Wind Direction: [~a-zA-Z ]*, Wind Speed \(mph\): [-\d~\.]{1,6}, Precipitation \(in\): [-\d~\.]{1,7}, Weather Condition: ([a-zA-Z ]*|~)$"""

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
                                          .appName("pl.michalsz.spark.FactLoader")
                                          .getOrCreate()
    val filesLocation = args(0)

    val accidentDS: Dataset[ParsedAccident] = getAccidentsDS(filesLocation)

    val weatherDS: Dataset[ParsedWeather] = getWeatherDS(filesLocation)
      .sort("weatherAirportCode", "dateTime")
    val windowAirport = Window.partitionBy(weatherDS("weatherAirportCode")).orderBy(weatherDS("dateTime"))
    val windowedWeatherDS = weatherDS
      .withColumn("dateTimeLag", lag(weatherDS("dateTime"), 1, Timestamp.valueOf(LocalDateTime.of(1970, 1, 1, 0, 0))).over(windowAirport))
      .withColumn("dateTimeLead", lead(weatherDS("dateTime"), 1, Timestamp.valueOf(LocalDateTime.of(1970, 1, 1, 0, 0))).over(windowAirport))

    val weatherConditionDS = spark.read.table("WeatherCondition").as(Encoders.product[WeatherCondition])
    val visibilityDS = spark.read.table("Visibility").as(Encoders.product[Visibility])
    val temperatureDS = spark.read.table("Temperature").as(Encoders.product[Temperature])
    val surroundingDS = spark.read.table("Surrounding").as(Encoders.product[Surrounding])
    val locationDS = spark.read.table("Location").as(Encoders.product[Location])


    val accidentWithWeatherDS = accidentDS
      .join(windowedWeatherDS, accidentDS("airportCode") === windowedWeatherDS("weatherAirportCode")
        && abs(accidentDS("startTime").cast("Long") - windowedWeatherDS("dateTime").cast("Long")) < abs(accidentDS("startTime").cast("long") - windowedWeatherDS("dateTimeLag").cast("Long"))
        && abs(accidentDS("startTime").cast("Long") - windowedWeatherDS("dateTime").cast("Long")) <= abs(accidentDS("startTime").cast("long") - windowedWeatherDS("dateTimeLead").cast("Long"))
            , "left")
      .select(accidentDS("id"),
              accidentDS("distance"),
              accidentDS("severity"),
              accidentDS("street"),
              accidentDS("zipcode"),
              accidentDS("crossing"),
              accidentDS("railway"),
              accidentDS("stop"),
              accidentDS("startTime"),
              accidentDS("endTime"),
              windowedWeatherDS("temperature"),
              windowedWeatherDS("visibility"),
              windowedWeatherDS("weatherCondition")
              )

    val weatherAccidentsWithStatic = accidentWithWeatherDS
      .join(visibilityDS,
            accidentWithWeatherDS("visibility") >= visibilityDS("minimumDistance") &&
              accidentWithWeatherDS("visibility") < visibilityDS("maximumDistance"), "left")
      .na.fill(visibilityUnknownId, Seq("VisibilityId"))
      .join(temperatureDS,
            accidentWithWeatherDS("temperature") >= temperatureDS("minimumTemperature") &&
              accidentWithWeatherDS("temperature") < temperatureDS("maximumTemperature"), "left")
      .na.fill(temperatureUnknownId, Seq("temperatureId"))
      .join(surroundingDS,
            accidentWithWeatherDS("stop") === surroundingDS("stop") &&
              accidentWithWeatherDS("railway") === surroundingDS("railway") &&
              accidentWithWeatherDS("crossing") === surroundingDS("crossing"))
      .na.fill("Unknown", Seq("weatherCondition"))

    val weatherAccidents = weatherAccidentsWithStatic
      .join(weatherConditionDS, weatherAccidentsWithStatic("weatherCondition") === weatherConditionDS("description"))
      .select(
        weatherAccidentsWithStatic("id"),
        weatherAccidentsWithStatic("startTime"),
        weatherAccidentsWithStatic("endTime"),
        weatherAccidentsWithStatic("zipcode"),
        weatherAccidentsWithStatic("street"),
        weatherAccidentsWithStatic("distance"),
        weatherAccidentsWithStatic("severity"),
        weatherAccidentsWithStatic("visibilityId"),
        weatherAccidentsWithStatic("temperatureId"),
        weatherAccidentsWithStatic("surroundingId"),
        weatherConditionDS("weatherConditionId")
        )

    def extractDaysBetween = (start: Date, end: Date) => {
      start.toLocalDate
           .toEpochDay
           .to(end.toLocalDate.toEpochDay)
           .map(LocalDate.ofEpochDay)
           .toList
    }
    val extractDaysBetweenUDF = udf(extractDaysBetween)

    weatherAccidents.join(locationDS,
                          weatherAccidents("zipcode") === locationDS("zipcode") &&
                            weatherAccidents("street") === locationDS("street"))
                    .withColumn("AccidentIdHll", hll_init("id"))
                    .withColumn("dateArray", extractDaysBetweenUDF(to_date(col("startTime")), to_date(col("endTime"))))
                    .withColumn("timeId", explode(col("dateArray")))
                    .groupBy(
                      locationDS("locationId"),
                      weatherAccidents("surroundingId"),
                      weatherAccidents("severity"),
                      col("timeId"),
                      weatherAccidents("visibilityId"),
                      weatherAccidents("weatherConditionId"),
                      weatherAccidents("temperatureId")
                      )
                    .agg(
                      sum("distance").as("distance"),
                      hll_merge("accidentIdHll").as("numberOfAccidents")
                      )
                    .withColumn("accidentId", monotonically_increasing_id)
                    .select(
                      col("accidentId"),
                      col("distance"),
                      col("numberOfAccidents"),
                      col("severity"),
                      col("timeId"),
                      col("locationId"),
                      col("surroundingId"),
                      col("temperatureId"),
                      col("visibilityId"),
                      col("weatherConditionId")
                      )
                    .write
                    .format("delta")
                    .insertInto("Accident")

  }

  private def getAccidentsDS(filesLocation: String): Dataset[ParsedAccident] = {
    val spark: SparkSession = SparkSession.builder()
                                          .getOrCreate()
    import spark.implicits._

    val fileAccidentDS = spark.read
                              .option("header", value = true)
                              .option("quote", "\"")
                              .option("header", "true")
                              .schema(Encoders.product[FileAccident].schema)
                              .csv(s"$filesLocation/main*")
                              .as[FileAccident]

    fileAccidentDS
      .select(
        fileAccidentDS("id"),
        fileAccidentDS("startTime"),
        fileAccidentDS("endTime"),
        fileAccidentDS("distance"),
        fileAccidentDS("severity"),
        fileAccidentDS("street"),
        fileAccidentDS("zipcode"),
        fileAccidentDS("crossing"),
        fileAccidentDS("railway"),
        fileAccidentDS("stop"),
        fileAccidentDS("airportCode")
        ).as(Encoders.product[ParsedAccident])
  }

  private def getWeatherDS(filesLocation: String): Dataset[ParsedWeather] = {
    val spark: SparkSession = SparkSession.builder()
                                          .getOrCreate()

    val weatherDF = spark.read.textFile(s"$filesLocation/weather*").toDF("value")
    weatherDF.filter(col("value").rlike(weatherRegex))
             .select(
               functions.split(functions.regexp_replace(col("value"), weatherRegex, "$1T$2,$3,$4,$5,$6"), ",")
                        .as("weather_value"))
             .withColumn("dateTime", to_timestamp(col("weather_value")(0)))
             .withColumn("weatherAirportCode", col("weather_value")(1))
             .withColumn("temperature", col("weather_value")(2).cast("Double"))
             .withColumn("visibility", col("weather_value")(3).cast("Double"))
             .withColumn("weatherCondition", col("weather_value")(4))
             .drop("weather_value")
             .na.replace(Seq("temperature", "visibility"), Map("~" -> null))
             .as(Encoders.product[ParsedWeather])
  }
}
