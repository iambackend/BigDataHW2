import java.util.Locale

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession

case object Session {
    // Windows dependency
    if (System.getProperty("os.name") == "Windows 10")
        System.setProperty("hadoop.home.dir", "F:\\Workspace\\Projects\\IBD-HW2\\extra")
    // Set Locale
    Locale.setDefault(Locale.US)
    // Remove INFO messages before session starts
    LogManager.getLogger("org").setLevel(Level.WARN)
    // Init session
    val Spark: SparkSession = SparkSession.builder()
      .appName("Sentiment Classifier")
      .master("yarn")
      .getOrCreate()
    // Remove INFO messages
    Spark.sparkContext.setLogLevel("WARN")
}
