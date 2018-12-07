import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

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


    // load model
    val theBestModel: ALSModel = ALSModel.load("src/main/resources/model")


    combinedTitleRatingUser.cache()

    combinedTitleRatingUser.createOrReplaceTempView("trainingData")
    probeDF.createOrReplaceTempView("probe")
    qualifyingDF.createOrReplaceTempView("qualifying")

    println("probe count:" + probeDF.count())
    println("qualifying count:" + qualifyingDF.count())
    println("training data count: " + combinedTitleRatingUser.count())

    val probeTrainingSubset = spark.sql("SELECT * FROM trainingData LEFT SEMI JOIN probe ON (trainingData.movieId = probe.movieId " +
                                                                                              " AND trainingData.userId = probe.userId )")
    println("probe subset count: " + probeTrainingSubset.count())
    import spark.sqlContext.implicits._
    probeTrainingSubset.printSchema()
    probeTrainingSubset.sort($"movieId".asc, $"userId".desc).show(100)
    probeDF.sort($"movieId".asc, $"userId".desc).show(100)


    // evaluation

    val evaluatorRMSE = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")

    // probe predictions
    val probePredictions = theBestModel.transform(probeTrainingSubset)
    probePredictions.show(50)
    val rmse = evaluatorRMSE.evaluate(probePredictions )
    println(f"Root-mean-square error = $rmse%1.4f" + "                 Is larger better? " + evaluatorRMSE.isLargerBetter)

  }

}
