import org.apache.spark.sql.SparkSession

case object Session {
    if (System.getProperty("os.name") == "Windows 10")
        System.setProperty("hadoop.home.dir", "F:\\Workspace\\Projects\\IBD-HW2\\extra")

    val Spark: SparkSession = SparkSession.builder()
      .appName("Sentiment Classifier")
      .master("local")
      .getOrCreate()
}
