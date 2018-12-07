import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/*
 * Created by © Matko Soric.
 */

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


    // loading data
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

    val probeSchema = new StructType()
      .add(StructField("movieId", LongType, true))
      .add(StructField("userId", LongType, true))

    val probeDF = spark.read
      .option("header", "false")
      .schema(probeSchema)
      .csv("src/main/resources/probe.csv")

    val qualifyingSchema = new StructType()
      .add(StructField("movieId", LongType, true))
      .add(StructField("userId", LongType, true))
      .add(StructField("date", DateType, true))

    val qualifyingDF = spark.read
      .option("header", "false")
      .schema(qualifyingSchema)
      .csv("src/main/resources/qualifying.csv")



    // data set exploration

    combinedTitleRatingUser.createOrReplaceTempView("training")
    probeDF.createOrReplaceGlobalTempView("probe")
    qualifyingDF.createOrReplaceGlobalTempView("qualifying")


    println("Sample data: ")
    spark.sql("SELECT *, rand() as random FROM training order by random").drop("random").show(40, false)

    val numberOfReviews = combinedTitleRatingUser.count()
    val numberOfUsers = combinedTitleRatingUser.select("userId").distinct().count()
    val numberOfMovies = combinedTitleRatingUser.select("movieId").distinct().count()

    println(s"In our complete data set we have $numberOfReviews reviews, performed by $numberOfUsers users, on a collection of $numberOfMovies movies. \n")

    println("Standard data set statistics:")
    combinedTitleRatingUser.describe().show()

    println("Top 20 movies by average score, with minimum and maximum score, and number of reviews:")
    spark.sql("SELECT " +
      "title, " +
      "MIN(rating) AS minScore, " +
      "MAX(rating) AS maxScore, " +
      "ROUND(AVG(rating), 3) AS averageScore, " +
      "count(1) AS numReviews " +
      "FROM training " +
      "GROUP BY title " +
      "ORDER BY averageScore DESC")
      .show(20, false)

    println("Twenty movies the the smallest number of reviews: ")
    spark.sql("SELECT title, count(1) AS numReviews FROM training GROUP BY title ORDER BY numReviews ASC").show(20, false)


    println("Five users with the smallest number of ratings:")
    spark.sql("SELECT userId, count(1) AS numberOfReviews FROM training GROUP BY userId ORDER BY numberOfReviews ASC LIMIT 5").show(20, false)

    println("Five users with the largest number of ratings:")
    spark.sql("SELECT userId, count(1) AS numberOfReviews FROM training GROUP BY userId ORDER BY numberOfReviews DESC LIMIT 5").show(20, false)


    println("Probe data set sample:")
    probeDF.distinct().show(15)

    println("Qualifying data set sample:")
    qualifyingDF.distinct().show(15)

  }

}
