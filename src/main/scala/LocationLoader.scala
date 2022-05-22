import model.{FileAccident, FileLocation, Location}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.{Encoders, SparkSession}

object LocationLoader {

  val REGION_NAMES = List("Central", "Eastern", "Mountain", "Pacific")

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
                                          .appName("LocationLoader")
                                          .getOrCreate()

    val filesLocation = spark.sparkContext.broadcast(args(0))
    val bigQueryTemporaryGcsBucket = spark.sparkContext.broadcast(args(1))
    val bigQueryDataset = spark.sparkContext.broadcast(args(2))

    REGION_NAMES.par.foreach(region => addLocationForCsv(filesLocation, bigQueryTemporaryGcsBucket, bigQueryDataset, region))
  }

  private def addLocationForCsv(filesLocation: Broadcast[String], bigQueryTemporaryGcsBucket: Broadcast[String], bigQueryDataset: Broadcast[String], regionName: String): Unit = {
    val spark: SparkSession = SparkSession.builder()
                                          .getOrCreate()
    import spark.implicits._

    val geoDS = spark.read
                     .option("header", value = true)
                     .option("quote", "\"")
                     .option("header", "true")
                     .schema(Encoders.product[FileLocation].schema)
                     .csv(s"${filesLocation.value}/geoData$regionName.csv")
                     .as[FileLocation]

    val accidentDS = spark.read
                          .option("header", value = true)
                          .option("quote", "\"")
                          .option("header", "true")
                          .schema(Encoders.product[FileAccident].schema)
                          .csv(s"${filesLocation.value}/mainData$regionName.csv")
                          .as[FileAccident]

    val locationDS = geoDS.join(accidentDS, geoDS("zipcode") === accidentDS("zipcode"))
                          .select(
                            geoDS("zipcode"),
                            accidentDS("airportCode"),
                            geoDS("city"),
                            geoDS("county"),
                            geoDS("state"),
                            geoDS("country"),
                            accidentDS("street")
                            )
                          .distinct()
                          .withColumn("locationId", monotonically_increasing_id)
                          .select("locationId", "zipcode", "airportCode", "city", "county", "state", "country", "street")
                          .as[Location]

    locationDS.write
              .format("bigquery")
              .option("temporaryGcsBucket", bigQueryTemporaryGcsBucket.value)
              .insertInto(s"${bigQueryDataset.value}.Location")
  }
}
