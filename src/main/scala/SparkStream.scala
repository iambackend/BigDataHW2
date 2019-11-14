import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, split}


object SparkStream {
    def main(args: Array[String]): Unit = {

        val spark:SparkSession = SparkSession.builder()
          .master("local[3]")
          .appName("IBD-HW2")
          .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        val df = spark.readStream
          .format("socket")
          .option("host","10.90.138.32")
          .option("port","8989")
          .load()

        val wordsDF = df.select("value")
        val query = wordsDF.writeStream
          .format("console")
          .outputMode("append")
          .option("truncate", "false")
          .start()
          .awaitTermination()
    }
}