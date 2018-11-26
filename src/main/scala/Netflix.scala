object Netflix {

  def main(args: Array[String]): Unit = {

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Netflix Recommendation System")
      .getOrCreate;


    val textFile = spark.sparkContext.textFile("src/main/resources/test.csv")
    val header = textFile.first()

    textFile.filter( row => !row.contains(":") ).foreach(println)



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
