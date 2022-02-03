package pl.michalsz.spark

import org.apache.spark.sql.SparkSession


object SurroundingLoader {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
                                          .appName("pl.michalsz.spark.SurroundingLoader")
                                          .getOrCreate()
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
         .format("delta")
         .insertInto("Surrounding")
  }

}
