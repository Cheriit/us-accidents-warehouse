package pl.michalsz.spark

import org.apache.spark.sql.SparkSession

object VisibilityLoader {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
                                          .appName("pl.michalsz.spark.VisibilityLoader")
                                          .getOrCreate()

    val VisibilityValues = Seq(
      (0, Some(0), Some(2), "Terrible"),
      (1, Some(2), Some(4), "Bad"),
      (2, Some(4), Some(6), "Average"),
      (3, Some(6), Some(8), "Good"),
      (4, Some(8), Some(127), "Excellent"),
      (5, None, None, "Unknown")
      )

    spark.createDataFrame(VisibilityValues)
         .toDF("VisibilityId", "MinimumDistance", "MaximumDistance", "Description")
         .write
         .format("delta")
         .insertInto("Visibility")
  }
}
