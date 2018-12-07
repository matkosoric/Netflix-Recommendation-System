import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, LongType, StructField, StructType}

/*
 * Created by © Matko Soric.
 */

object Preprocessing2 {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Netflix Recommendation System - Preprocessing 2")
      .getOrCreate;

    val schema = new StructType()
      .add(StructField("movieId", LongType, true))
      .add(StructField("userId", LongType, true))
      .add(StructField("rating", LongType, true))
      .add(StructField("date", DateType, true))

    val fourElementLogs = spark.read
      .format("com.databricks.spark.csv")
      .option("dateFormat", "yyyy-MM-dd")
      .schema(schema)
      .load("full_logs/part-*.txt")

//    spark.sparkContext.getConf.set("spark.sql.parquet.compression.codec", "snappy")
    fourElementLogs
      .coalesce(1)
      .write
      .option("header","true")
      .mode("overwrite")
      .parquet("netflix-data-parquet")

  }

}
