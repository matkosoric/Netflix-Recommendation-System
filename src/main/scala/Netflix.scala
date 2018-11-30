
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util.Calendar

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.joda.time.Minutes

import scala.util.Random

object Netflix {

  def main(args: Array[String]): Unit = {

    val start = Calendar.getInstance().getTime()

    val spark: SparkSession = org.apache.spark.sql.SparkSession.builder
      .master("local")
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

//    println(fourElementsDF.count())

    val movieIdTitleSchema = new StructType()
      .add(StructField("movieId", LongType, true))
      .add(StructField("year", LongType, true))
      .add(StructField("title", StringType, true))

    val movieIdTitles = spark.read
      .option("header", "false")
      .schema(movieIdTitleSchema)
      .csv("netflix-data/movie_titles.txt")

//    println(movieIdTitles.count())

    val combinedTitleRatingUser = fourElementsDF
      .join(movieIdTitles, usingColumn = "movieId")

//    combinedTitleRatingUser.show()
//    combinedTitleRatingUser.describe()


    // dataset exploration
    combinedTitleRatingUser.createOrReplaceTempView("netflix")
    spark.sql("SELECT * FROM netflix").show(40, false)

//    val numberOfReviews = spark.sql("SELECT COUNT (1) FROM netflix")
    val numberOfReviews2 = combinedTitleRatingUser.count()

//    val numberOfUsers = spark.sql("SELECT COUNT (DISTINCT userId) FROM netflix")
    val numberOfUsers2 = combinedTitleRatingUser.select("userId").distinct().count()

//    val numberOfMovies = spark.sql("SELECT COUNT (DISTINCT movieId) FROM netflix")
    val numberOfMovies2 = combinedTitleRatingUser.select("movieId").distinct().count()

    println (s"We have $numberOfReviews2 reviews, performed by $numberOfUsers2 users, on a collection of $numberOfMovies2 movies \n\n")

    println("Top 50 movies by minimum, maxminum, and average score, and number of reviews")
    spark.sql("SELECT title, MIN(rating) AS minScore, MAX(rating) AS maxScore, ROUND(AVG(rating), 3) AS averageScore, count(1) AS numReviews " +
      "FROM netflix " +
      "GROUP BY title " +
      "ORDER BY averageScore DESC").show(50, false)



//    1‰ subdataset for speed
    val Array(combinedTitleRatingUser2, dropping) = combinedTitleRatingUser.randomSplit(Array(0.001, 0.999), 125)
    val Array(training, test) = combinedTitleRatingUser2.randomSplit(Array(0.8, 0.2), 73)

    // COMPLETE DATASET
//    val Array(training, test) = combinedTitleRatingUser.randomSplit(Array(0.8, 0.2), 73)


    test.describe().show()
    training.describe().show()


    val model = new ALS()
//      .setSeed(Random.nextLong())
      .setSeed(555)
//      .setImplicitPrefs(true)
//      .setRank(10)
      .setRegParam(0.01)
//      .setAlpha(1.0)
      .setMaxIter(5)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
      .setPredictionCol("prediction")
      .fit(training)
      .setColdStartStrategy("drop")

////    val als = new ALS()
////      .setMaxIter(5)
////      .setRegParam(0.01)
////      .setUserCol("userId")
////      .setItemCol("movieId")
////      .setRatingCol("rating")
////    val model = als.fit(training)
////    model.setColdStartStrategy("drop")
//
//
    val predictions = model.transform(test)
    predictions.show(200, false)


//
//    val trainPredictionsAndLabels =
//      model.transform(training).select("rating",
//        "prediction").map { case Row(rating: Double, prediction: Double) => (rating,
//        prediction) }(null).rdd
//
//
//    val regressionMetrics = new RegressionMetrics(trainPredictionsAndLabels)
//    println(s"ExplainedVariance = ${regressionMetrics.explainedVariance}")
//    println(s"MeanAbsoluteError = ${regressionMetrics.meanAbsoluteError}")
//    println(s"MeanSquaredError = ${regressionMetrics.meanSquaredError}")
//    println(s"RootMeanSquaredError = ${regressionMetrics.rootMeanSquaredError}")
//    println(s"R-squared = ${regressionMetrics.r2}")



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

    println(s"Root-mean-square error = $rmse" + evaluatorRMSE.isLargerBetter)
    println(s"Mean-square error = $mse")
    println(s"Unadjusted coefficient of determination = $r2")



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


    val minuteFormat = new SimpleDateFormat("mm")
    println("Start time: " + start)
    val end = Calendar.getInstance().getTime()
    println("End time:  " + end)
    println ("Difference in minutes: " + (minuteFormat.format(start).toInt - minuteFormat.format(end).toInt))

    spark.stop()

  }

}
