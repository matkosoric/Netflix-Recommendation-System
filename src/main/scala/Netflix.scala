
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import scala.util.Random

object Netflix {

  def main(args: Array[String]): Unit = {

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

//    combinedTitleRatingUser.createOrReplaceTempView("netflix")
//    spark.sql("SELECT title, AVG(rating) AS score FROM netflix GROUP BY title ORDER BY score DESC").show(40, false)

    val Array(combinedTitleRatingUser2, dropping) = combinedTitleRatingUser.randomSplit(Array(0.001, 0.999), 125)

    val Array(training, test) = combinedTitleRatingUser2.randomSplit(Array(0.8, 0.2), 8674234)

    //    training.cache()
    //    test.cache()

    //    println(training.count())
    //    println(test.count())

    //    training.describe()
    //    test.describe()

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

//    val als = new ALS()
//      .setMaxIter(5)
//      .setRegParam(0.01)
//      .setUserCol("userId")
//      .setItemCol("movieId")
//      .setRatingCol("rating")
//    val model = als.fit(training)
//    model.setColdStartStrategy("drop")


    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)

    println(s"Root-mean-square error = $rmse")

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


    spark.stop()

  }

}
