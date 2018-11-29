
import java.util

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._

object Netflix {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Netflix Recommendation System")
      .getOrCreate;

    val fourElementsSchema = new StructType()
      .add(StructField("movieId", LongType, true))
      .add(StructField("userId", LongType, true))
      .add(StructField("rating", LongType, true))
      .add(StructField("date", DateType, true))

    val fourElementsDF = spark.read
      .option("header", "true")
      .schema(fourElementsSchema)
      .parquet("netflix-data-parquet")

//    println(fourElementsDF.count())

    val movieIdTitleSchema = new StructType()
      .add(StructField("movieId", LongType, true))
      .add(StructField("year", LongType, true))
      .add(StructField("title", StringType, true))

    val movieIdTitles = spark.read
      .option("header", "true")
      .schema(movieIdTitleSchema)
      .csv("netflix-data/movie_titles.txt")

//    println(movieIdTitles.count())

    val combinedTitleRatingUser = fourElementsDF
      .join(movieIdTitles, usingColumn = "movieId")

    import spark.implicits._

    combinedTitleRatingUser
//      .select(combinedTitleRatingUser.col("*"))
      .select($"movieId", $"title").distinct()
      .filter("year = 1979")
      .show(3000, false)


  }

}
