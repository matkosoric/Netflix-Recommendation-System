
import java.util.Calendar
import java.util.concurrent.TimeUnit

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


object Netflix {

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

//    0.01‰ subdataset for speed
//    val Array(combinedTitleRatingUser2, dropping) = combinedTitleRatingUser.randomSplit(Array(0.0001, 0.9999), 235)
//    val Array(training, test) = combinedTitleRatingUser2.randomSplit(Array(0.8, 0.2), 544)

    // COMPLETE DATA SET
//    val Array(training, test) = combinedTitleRatingUser.randomSplit(Array(0.8, 0.2), 73)

    // COMPLETE DATA SET FOR K-FOLD validation
    val training = combinedTitleRatingUser.drop ("year", "title", "random")

    System.gc()

    //    test.cache()
    training.cache()
    training.printSchema()

//    test.describe().show()
//    training.describe().show()


    val alsModel = new ALS()
      .setSeed(555)
      .setCheckpointInterval(5)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
      .setPredictionCol("prediction")
//      .fit(training)
      .setColdStartStrategy("drop")

    println("\nModel parameters explanations: \n" + alsModel.explainParams)




    // evaluation
//
//    val predictions = model.transform(test)
//    predictions.show(30, false)
//
    val evaluatorRMSE = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
//    val evaluatorMSE = new RegressionEvaluator()
//      .setMetricName("mse")
//      .setLabelCol("rating")
//      .setPredictionCol("prediction")
//    val evaluatorR2 = new RegressionEvaluator()
//      .setMetricName("r2")
//      .setLabelCol("rating")
//      .setPredictionCol("prediction")
//
//    val rmse = evaluatorRMSE.evaluate(predictions)
//    val mse = evaluatorMSE.evaluate(predictions)
//    val r2 = evaluatorR2.evaluate(predictions)
//    println(f"Root-mean-square error = $rmse%1.10f" + "                 Is larger better? " + evaluatorRMSE.isLargerBetter)
//    println(f"Mean-square error = $mse%1.10f" + "                       Is larger better? " + evaluatorMSE.isLargerBetter)
//    println(f"Unadjusted coefficient of determination = $r2%1.10f" + "  Is larger better? " + evaluatorR2.isLargerBetter)


    // k-fold validation
    val paramGrid = new ParamGridBuilder()
      .addGrid(alsModel.implicitPrefs, Array(true, false))
      .addGrid(alsModel.rank, Array(3, 4))
      .addGrid(alsModel.alpha, Array(0.6, 1.0, 2.0))
      .addGrid(alsModel.maxIter, Array(10, 15, 20))
      .build()


//    val pipeline = new Pipeline()
//      .setStages(Array(model))

    val cv = new CrossValidator()
      .setEstimator(alsModel)
      .setEvaluator(evaluatorRMSE)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)
//      .setParallelism(2)


    val best_als : ALSModel = cv.fit(training).bestModel.asInstanceOf[ALSModel]


    best_als.write.overwrite().save("exporting_model_ALS_6_12" + System.nanoTime())
    best_als.save("exporting_model_ALS_backup_6_12" + System.nanoTime())


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