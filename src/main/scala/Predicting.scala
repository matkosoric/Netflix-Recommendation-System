import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.lit

object Predicting {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = org.apache.spark.sql.SparkSession.builder
      .master("local[6]")
      .config("spark.sql.warehouse.dir", "spark-warehouse")
      .config("spark.local.dir", "/home/spark-intermediate")
      .appName("Netflix Recommendation System - Predicting")
      .getOrCreate;

    spark.sparkContext.setLogLevel("WARN")   // WARN, INFO, DEBUG
    spark.sparkContext.setCheckpointDir("spark-checkpoint")

    // load data
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





    probeDF.show(20)
    qualifyingDF.show(20)

    // load model
//    val theBestModel: ALSModel = ALSModel.load("exporting_model_ALS_backup_6_12111123848888991")

//    println(theBestModel.extractParamMap())



    // load qualifying data set



    //    // concrete predictions
    //
    //    // SLOW !!!
    ////    val userRecs = model.recommendForAllUsers(10)
    ////    val movieRecs = model.recommendForAllItems(10)
    //    // Generate top 10 movie recommendations for a specified set of users
    //    val users = combinedTitleRatingUser.select(model.getUserCol).distinct().limit(3)
    //    val userSubsetRecs = model.recommendForUserSubset(users, 10)
    //    // Generate top 10 user recommendations for a specified set of movies
    //    val movies = combinedTitleRatingUser.select(model.getItemCol).distinct().limit(3)
    //    val movieSubSetRecs = model.recommendForItemSubset(movies, 10)
    //
    //    users.show(false)
    //    userSubsetRecs.show(false)
    //    movies.show(false)
    //    movieSubSetRecs.show(false)


  }

}
