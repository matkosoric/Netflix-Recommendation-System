import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, SparkSession}

object Preprocessing1 {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Netflix Recommendation System - Preprocessing 1")
      .getOrCreate;
    import spark.implicits._

    val fs = FileSystem.get (new Configuration())
    val files = fs.listStatus(new Path("netflix-data/training_set/")).map(x=> x.getPath.toString).filter ( row => row.contains(".txt"))

    files.foreach(filename => {

      val content = spark.sparkContext.textFile(filename)
      val movieId = filename.substring(filename.indexOf("mv_") + 3, filename.indexOf(".txt")).replaceFirst("^0+(?!$)", "")
      val newLogs = content.map(line => ((movieId + "," + line))).toDF()

      val logs2 = newLogs.filter(row => !row.toString().contains(":"))

      logs2.write.mode(SaveMode.Append).text("full_logs")

    })

  }

}

