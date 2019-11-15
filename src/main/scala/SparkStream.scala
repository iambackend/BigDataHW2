import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{explode, split}
import org.apache.spark.sql.functions.lower


object SparkStream {
    def main(args: Array[String]): Unit = {

        val spark:SparkSession = SparkSession.builder()
          .master("local[3]")
          .appName("IBD-HW2")
          .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        val df = spark.readStream
          .format("socket")
          .option("host", "10.90.138.32")
          .option("port", "8989")
          .load()

        val wordsDF = df.select(explode(split(df("value"),"[ !?,.\"]")).alias("word"))
        val words = wordsDF.groupBy(lower(wordsDF("word"))).count()
        val query = words.writeStream
          .format("console")
          .outputMode("complete")
          .option("truncate", "false")
          .start()
          .awaitTermination()
    }
}