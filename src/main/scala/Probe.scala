import java.io.FileInputStream

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import scala.util.control.Breaks._

import scala.io.{BufferedSource, Source}

object Probe {


  def main(args: Array[String]): Unit = {

    val spark: SparkSession = org.apache.spark.sql.SparkSession.builder
      .master("local[6]")
      .appName("Netflix Recommendation System - Probe data set")
      .getOrCreate;

    spark.sparkContext.setLogLevel("WARN")   // WARN, INFO, DEBUG

    import spark.implicits._


    val completeFile = spark.sparkContext.textFile("netflix-data/probe.txt")

//    val contentArray = completeFile.collect()
    val contentArray = completeFile.take(100)

    val schema = StructType(
      StructField("value", StringType, true) :: Nil)

    var finalDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)



    for (i <- 0 to contentArray.length -1 ) {
    println("index:" + i)

      if (contentArray(i).contains(":")) {

        var i2 = i+1
        breakable {
          while ((i2 < contentArray.length)) {
            if (!contentArray(i2).contains(":")) {
              val logs = spark.sparkContext.parallelize(Seq(contentArray(i).substring(0, (contentArray(i).length() - 1)) + ", " + contentArray(i2))).toDF()
              //          logs.write.mode(SaveMode.Append).text("src/main/resources/data/probe" + System.nanoTime())
              finalDF = finalDF.union(logs)
              println("Processing line: " + i2)
            } else break
            i2 += 1
          }
        }
      }
    }

    finalDF
      .coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .text("src/main/resources/data/probe" + System.nanoTime())

  }
}
