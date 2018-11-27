import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, LongType, StructField, StructType}



object Preprocessing {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Netflix Recommendation System")
      .getOrCreate;
    import spark.implicits._


        val allFiles = spark.sparkContext.wholeTextFiles("netflix-data/training_set/").collect()
//      .take(2000)

    val files = allFiles
//      .take(2000)
      .map { case (filename, content) => filename }


//    spark.sparkContext.broadcast(files)


    val logs = files.map(filename => {

      val content = spark.sparkContext.textFile(filename)
      val movieId = filename.substring(filename.indexOf("mv_") + 3, filename.indexOf(".txt")).replaceFirst("^0+(?!$)", "")
      val newLogs = content.map(line => ((movieId + "," + line))).toDF()
      newLogs
    }).reduce(_ union _)

    val logs2 = logs.filter(row => !row.toString().contains(":"))

    logs2
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet("intermediate.parquet")



  }

}

