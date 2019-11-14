import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession


case object Session {
    // Windows dependency
    if (System.getProperty("os.name") == "Windows 10")
        System.setProperty("hadoop.home.dir", "F:\\Workspace\\Projects\\IBD-HW2\\extra")
    // Remove INFO messages
    LogManager.getLogger("org").setLevel(Level.WARN)
    LogManager.getLogger("com").setLevel(Level.WARN)
    // Init session
    val Spark: SparkSession = SparkSession.builder()
      .appName("Sentiment Classifier")
      .master("local")
      .getOrCreate()
}
