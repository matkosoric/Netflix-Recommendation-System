import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, LongType, StructField, StructType}

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
//      .option("dateFormat", "yyyy-MM-dd")
      .schema(schema)
//        .csv("full_logs/part-00000-96491c87-24d2-44fb-bc42-15e9ddca1bac-c000.csv")
      .load("full_logs/part-00000-96639f73-d907-4c48-bbdb-0cb8b550be72-c000.csv")

    fourElementLogs.toDF().show()

    fourElementLogs.toDF().coalesce(1)

  }

}
