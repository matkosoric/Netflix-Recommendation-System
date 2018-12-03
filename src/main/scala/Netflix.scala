
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.TimeUnit

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object Netflix {

  def main(args: Array[String]): Unit = {

    val startNano = System.nanoTime()
    val startHumanReadable = Calendar.getInstance().getTime()
    println("Start time: " + startHumanReadable)

    val spark: SparkSession = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .config("spark.sql.warehouse.dir", "spark-warehouse")
      .appName("Netflix Recommendation System")
      .getOrCreate;

    spark.sparkContext.setLogLevel("WARN")

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
    spark.sql("SELECT DISTINCT (*) FROM netflix").show(40, false)

    val numberOfReviews = combinedTitleRatingUser.count()
    val numberOfUsers = combinedTitleRatingUser.select("userId").distinct().count()
    val numberOfMovies = combinedTitleRatingUser.select("movieId").distinct().count()
//        val numberOfReviews = spark.sql("SELECT COUNT (1) FROM netflix")
//        val numberOfUsers = spark.sql("SELECT COUNT (DISTINCT userId) FROM netflix")
//        val numberOfMovies = spark.sql("SELECT COUNT (DISTINCT movieId) FROM netflix")

    println (s"In our complete dataset we have $numberOfReviews reviews, performed by $numberOfUsers users, on a collection of $numberOfMovies movies \n\n")

    println("Top 50 movies by minimum, maxminum, and average score, and number of reviews")
    spark.sql("SELECT " +
                              "title, " +
                              "MIN(rating) AS minScore, " +
                              "MAX(rating) AS maxScore, " +
                              "ROUND(AVG(rating), 3) AS averageScore, " +
                              "count(1) AS numReviews " +
                      "FROM netflix " +
                      "GROUP BY title " +
                      "ORDER BY averageScore DESC").show(50, false)

    println("Top 100 movies with the lowest number of reviews:")
    spark.sql ("SELECT title, count(1) AS numReviews FROM netflix GROUP BY title ORDER BY numReviews ASC").show(100, false)

//    0.01‰ subdataset for speed
    val Array(combinedTitleRatingUser2, dropping) = combinedTitleRatingUser.randomSplit(Array(0.0001, 0.9999), 235)
    val Array(training, test) = combinedTitleRatingUser2.randomSplit(Array(0.8, 0.2), 544)

    // COMPLETE DATASET
//    val Array(training, test) = combinedTitleRatingUser.randomSplit(Array(0.8, 0.2), 73)


    test.cache()
    training.cache()

    System.gc()

//    test.describe().show()
//    training.describe().show()

    val model = new ALS()
      .setSeed(555)
      .setImplicitPrefs(true)
      .setRank(10)
      .setRegParam(0.01)
      .setAlpha(1.0)
      .setMaxIter(5)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
      .setPredictionCol("prediction")
      .fit(training)
      .setColdStartStrategy("drop")

    println("\n Model parameters explanations: \n" + model.explainParams)

////    val als = new ALS()
////      .setMaxIter(5)
////      .setRegParam(0.01)
////      .setUserCol("userId")
////      .setItemCol("movieId")
////      .setRatingCol("rating")
////    val model = als.fit(training)
////    model.setColdStartStrategy("drop")

    val predictions = model.transform(test)
    predictions.show(30, false)



    // evaluation
    val evaluatorRMSE = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val evaluatorMSE = new RegressionEvaluator()
      .setMetricName("mse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val evaluatorR2 = new RegressionEvaluator()
      .setMetricName("r2")
      .setLabelCol("rating")
      .setPredictionCol("prediction")



    val rmse = evaluatorRMSE.evaluate(predictions)
    val mse = evaluatorMSE.evaluate(predictions)
    val r2 = evaluatorR2.evaluate(predictions)

    println(f"Root-mean-square error = $rmse%1.2f" + ". Is larger better? " + evaluatorRMSE.isLargerBetter)
    println(f"Mean-square error = $mse%1.2f" + ". Is larger better? " + evaluatorMSE.isLargerBetter)
    println(f"Unadjusted coefficient of determination = $r2%1.2f" + ". Is larger better? " + evaluatorR2.isLargerBetter)


    // SLOW !!!
//    val userRecs = model.recommendForAllUsers(10)
//    val movieRecs = model.recommendForAllItems(10)


    // Generate top 10 movie recommendations for a specified set of users
    val users = combinedTitleRatingUser.select(model.getUserCol).distinct().limit(3)
    val userSubsetRecs = model.recommendForUserSubset(users, 10)
    // Generate top 10 user recommendations for a specified set of movies
    val movies = combinedTitleRatingUser.select(model.getItemCol).distinct().limit(3)
    val movieSubSetRecs = model.recommendForItemSubset(movies, 10)

    users.show(false)
    userSubsetRecs.show(false)
    movies.show(false)
    movieSubSetRecs.show(false)


    // running time
    val minuteFormat = new SimpleDateFormat("mm")
    println("Start time: " + startHumanReadable)
    val endNano = System.nanoTime()
    val endHumanReadable = Calendar.getInstance().getTime()
    println("End time:  " + endHumanReadable)
    println ("Running time in minutes: " + TimeUnit.MINUTES.convert(endNano - startNano, TimeUnit.NANOSECONDS))

    spark.stop()


  }

}



//System.gc()

//val zippedData = data.rdd.zipWithIndex()collect()