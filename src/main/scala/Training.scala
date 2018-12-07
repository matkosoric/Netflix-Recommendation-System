
import java.util.Calendar
import java.util.concurrent.TimeUnit

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/*
 * Created by © Matko Soric.
 */

object Training {

  def main(args: Array[String]): Unit = {

    // time metrics
    val startNano = System.nanoTime()
    val startHumanReadable = Calendar.getInstance().getTime()
    println("Start time: " + startHumanReadable)


    val spark: SparkSession = org.apache.spark.sql.SparkSession.builder
      .master("local[6]")
      .config("spark.sql.warehouse.dir", "spark-warehouse")
      .config("spark.local.dir", "/home/spark-intermediate")
      .appName("Netflix Recommendation System")
      .getOrCreate;

    spark.sparkContext.setLogLevel("WARN")   // WARN, INFO, DEBUG
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


    val training = combinedTitleRatingUser.drop ("year", "title", "random")
    System.gc()
    training.cache()
    training.printSchema()


    val alsModel = new ALS()
      .setSeed(555)
      .setCheckpointInterval(5)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
      .setPredictionCol("prediction")
      .setColdStartStrategy("drop")

    println("\nModel parameters explanations: \n" + alsModel.explainParams)


    // evaluation

    val evaluatorRMSE = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")

    // k-fold validation
    val paramGrid = new ParamGridBuilder()
      .addGrid(alsModel.implicitPrefs, Array(true, false))
      .addGrid(alsModel.rank, Array(3, 4))
      .addGrid(alsModel.alpha, Array(0.2, 0.4, 0.6, 1.0))
      .addGrid(alsModel.maxIter, Array(5, 7, 10, 15))
      .build()

    val cv = new CrossValidator()
      .setEstimator(alsModel)
      .setEvaluator(evaluatorRMSE)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(4)

    val best_als : ALSModel = cv.fit(training).bestModel.asInstanceOf[ALSModel]

    best_als.write.overwrite().save("exporting_model_ALS" + System.nanoTime())

    println(best_als.parent.extractParamMap())

    // time metrics
    println("Start time: " + startHumanReadable)
    val endNano = System.nanoTime()
    val endHumanReadable = Calendar.getInstance().getTime()
    println("End time:  " + endHumanReadable)
    println ("Running time in minutes: " + TimeUnit.MINUTES.convert(endNano - startNano, TimeUnit.NANOSECONDS))

    spark.stop()


  }

}