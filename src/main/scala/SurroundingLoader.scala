import org.apache.spark.sql.SparkSession


object SurroundingLoader {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
                                          .appName("SurroundingLoader")
                                          .getOrCreate()

    val bigQueryTemporaryGcsBucket = args(0)
    val bigQueryDataset = args(1)

    import scala.collection.mutable.ListBuffer

    val ids: Seq[Int] = 0 to 7
    val trueAndFalse = Seq(true, false)
    val valuesCombinations = new ListBuffer[(Boolean, Boolean, Boolean)]()
    for (valueCrossing <- trueAndFalse; valueRailway <- trueAndFalse; valueStop <- trueAndFalse) {
      valuesCombinations += ((valueCrossing, valueRailway, valueStop))
    }
    val valuesWithId = ids.zip(valuesCombinations)
                          .map(t => (t._1, t._2._1, t._2._2, t._2._3))

    spark.createDataFrame(valuesWithId)
         .toDF("SurroundingId", "Crossing", "Railway", "Stop")
         .write
         .format("bigquery")
         .option("temporaryGcsBucket", bigQueryTemporaryGcsBucket)
         .mode("append")
         .save(s"$bigQueryDataset.Surrounding")
  }

}
