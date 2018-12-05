import java.io.FileInputStream
import java.util.Calendar
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.util.control.Breaks._

object ProbeParsing {


  def main(args: Array[String]): Unit = {

    // time metrics
    val startNano = System.nanoTime()
    val startHumanReadable = Calendar.getInstance().getTime()
    println("Start time: " + startHumanReadable)


    val spark: SparkSession = org.apache.spark.sql.SparkSession.builder
      .master("local[6]")
      .appName("Netflix Recommendation System - Probe data set")
      .getOrCreate;

    spark.sparkContext.setLogLevel("WARN")   // WARN, INFO, DEBUG

    import spark.implicits._

    val completeFile = spark.sparkContext.textFile("netflix-data/probe.txt")

    val contentArray = completeFile.collect()
//    val contentArray = completeFile.take(10000)

    val schema = StructType(
      StructField("value", StringType, true) :: Nil)


    for (i <- 0 to contentArray.length -1 ) {

      if (contentArray(i).contains(":")) {

        var finalDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

        var i2 = i+1
        breakable {
          while ((i2 < contentArray.length)) {
            if (!contentArray(i2).contains(":")) {

              val newRow = contentArray(i).substring(0, (contentArray(i).length() - 1)) + ", " + contentArray(i2)

              val newDF = Seq(newRow).toDF()

              finalDF = finalDF.union(newDF)

//              val logs = spark.sparkContext.parallelize(Seq(contentArray(i).substring(0, (contentArray(i).length() - 1)) + ", " + contentArray(i2))).toDF()
//                        logs.write.mode(SaveMode.Append).text("src/main/resources/data/probe" + System.nanoTime())

            } else break
            i2 += 1
          }
        }

        println("Processing line: " + i)

        // flush data
          finalDF
            .coalesce(1)
            .write.mode(SaveMode.Append)
            .text("src/main/resources/data/probe")

      }

      System.gc()

    }


    // time metrics
    println("Start time: " + startHumanReadable)
    val endNano = System.nanoTime()
    val endHumanReadable = Calendar.getInstance().getTime()
    println("End time:  " + endHumanReadable)
    println ("Running time for ProbeParsing in minutes: " + TimeUnit.MINUTES.convert(endNano - startNano, TimeUnit.NANOSECONDS))


  }
}
