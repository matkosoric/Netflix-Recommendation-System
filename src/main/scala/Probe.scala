import java.io.FileInputStream

import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.{BufferedSource, Source}

object Probe {


  def main(args: Array[String]): Unit = {

    val spark: SparkSession = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Netflix Recommendation System - Probe data set")
      .getOrCreate;

    import spark.implicits._


    val completeFile = spark.sparkContext.textFile("netflix-data/probe.txt")

    val contentArray = completeFile.collect()

    for (i <- 0 to contentArray.take(100).length ) {

      if (contentArray(i).contains(":")) {

        var i2 = i
        while (!contentArray(i2+1).contains(":")) {
          i2 += 1

          val logs = spark.sparkContext.parallelize(Seq(contentArray(i).substring(0, (contentArray(i).length()-1)) + ", " + contentArray(i2))).toDF()
          logs.write.mode(SaveMode.Append).text("src/main/resources/probe")

        }


      }

    }

  }
}
