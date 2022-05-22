import org.apache.spark.sql.SparkSession

object TemperatureLoader {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
                                          .appName("TemperatureLoader")
                                          .getOrCreate()

    val bigQueryTemporaryGcsBucket = args(0)
    val bigQueryDataset = args(1)

    val temperatureValues = Seq(
      (0, Some(-128), Some(14), "Frosty"),
      (1, Some(14), Some(32), "Cold"),
      (2, Some(32), Some(59), "Warm"),
      (3, Some(59), Some(75), "Optimal"),
      (4, Some(75), Some(127), "Hot"),
      (5, None, None, "Unknown")
      )

    spark.createDataFrame(temperatureValues)
         .toDF("TemperatureId", "MinimumTemperature", "MaximumTemperature", "Description")
         .write
         .format("bigquery")
         .option("temporaryGcsBucket", bigQueryTemporaryGcsBucket)
         .insertInto(s"$bigQueryDataset.Temperature")
  }
}
