import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object Exploring {

  def main(args: Array[String]): Unit = {


    val spark: SparkSession = org.apache.spark.sql.SparkSession.builder
      .master("local[6]")
      .config("spark.sql.warehouse.dir", "spark-warehouse")
      .config("spark.local.dir", "/home/spark-intermediate")
      .appName("Netflix Recommendation System - Exploring the data set")
      .getOrCreate;

    spark.sparkContext.setLogLevel("WARN") // WARN, INFO, DEBUG
    spark.sparkContext.setCheckpointDir("spark-checkpoint")

    val fourElementsSchema = new StructType()
      .add(StructField("movieId", LongType, true))
      .add(StructField("userId", LongType, true))
      .add(StructField("rating", LongType, true))
      .add(StructField("date", DateType, true))

    val fourElementsDF = spark.read
      .option("header", "true")
      .schema(fourElementsSchema)
      .parquet("netflix-data-parquet")

    val movieIdTitleSchema = new StructType()
      .add(StructField("movieId", LongType, true))
      .add(StructField("year", LongType, true))
      .add(StructField("title", StringType, true))

    val movieIdTitles = spark.read
      .option("header", "false")
      .schema(movieIdTitleSchema)
      .csv("netflix-data/movie_titles.txt")

    val combinedTitleRatingUser = fourElementsDF
      .join(movieIdTitles, usingColumn = "movieId")



    // data set exploration

    combinedTitleRatingUser.createOrReplaceTempView("netflix")
    spark.sql("SELECT *, rand() as random FROM netflix order by random")
      .show(40, false)

    val numberOfReviews = combinedTitleRatingUser.count()
    val numberOfUsers = combinedTitleRatingUser.select("userId").distinct().count()
    val numberOfMovies = combinedTitleRatingUser.select("movieId").distinct().count()
    //        val numberOfReviews = spark.sql("SELECT COUNT (1) FROM netflix")
    //        val numberOfUsers = spark.sql("SELECT COUNT (DISTINCT userId) FROM netflix")
    //        val numberOfMovies = spark.sql("SELECT COUNT (DISTINCT movieId) FROM netflix")

    println(s"In our complete dataset we have $numberOfReviews reviews, performed by $numberOfUsers users, on a collection of $numberOfMovies movies \n\n")

    println("Top 20 movies by average score, with minimum and maxminum score, and number of reviews")
    spark.sql("SELECT " +
      "title, " +
      "MIN(rating) AS minScore, " +
      "MAX(rating) AS maxScore, " +
      "ROUND(AVG(rating), 3) AS averageScore, " +
      "count(1) AS numReviews " +
      "FROM netflix " +
      "GROUP BY title " +
      "ORDER BY averageScore DESC")
      .show(20, false)

    println("20 lowest ranked movies: ")
    spark.sql("SELECT title, count(1) AS numReviews FROM netflix GROUP BY title ORDER BY numReviews ASC").show(20, false)


  }

}
