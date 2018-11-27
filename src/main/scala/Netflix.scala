
import java.util

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._


object Netflix {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Netflix Recommendation System")
      .getOrCreate;
    import spark.implicits._



    val schema = new StructType()
      .add(StructField("movieId", LongType, true))
      .add(StructField("userId", LongType, true))
      .add(StructField("rating", LongType, true))
      .add(StructField("date", DateType, true))




    //    val loadingDF = spark.read
    //      .format("csv")
    //      //      .option("header", "false")
    ////            .option("header", "true")
    //            .option("mode", "DROPMALFORMED")
    //            .option("dateFormat", "yyyy-MM-dd")
    //      .option("delimiter", ",")
    ////            .option("inferSchema","true")
    //      .load("src/main/resources/test.csv")
    //
    //
    //    loadingDF.show(20);

  }

}
