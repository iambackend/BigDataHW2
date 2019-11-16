import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{current_timestamp, date_format}
import org.apache.spark.sql.streaming.OutputMode

object Main {
    private val Host = "10.90.138.32"
    private val Port = 8989

    val StreamColText = "sentences"
    val StreamColDateTime = "datetime"
    val OutputDir = "output/"

    def main(args: Array[String]): Unit = {
        //region Parse input
        if (args.length != 1) {
            println("Usage:\n<timeout in minutes>")
            return
        }
        var timeout: Long = 0
        try timeout = args(0).toLong * 60000
        catch {
            case _: NumberFormatException =>
                println("Cannot parse number of minutes")
                return
        }
        if (timeout < 0) {
            println("Number of minutes must be non-negative")
            return
        } else if (timeout == 0) {
            println("No timeout")
            timeout = -1
        }
        //endregion
        //region Initialization
        import Session.Spark
        import Spark.implicits._
        Classifier.Init()
        //endregion
        //region Start stream

        val data = Spark.readStream
          .format("socket")
          .option("host", Host)
          .option("port", Port)
          .load()
        val query = data.as[String].toDF(StreamColText)
          .withColumn(StreamColDateTime, date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
          .writeStream
          .foreachBatch {
              (batchDF: DataFrame, batchId: Long) =>
                  if (batchId > 0) {
                      batchDF.show(false)
                      Classifier.ProcessStream(batchDF)
                  }
          }
          .outputMode(OutputMode.Append())
          .start()
        query.awaitTermination(timeout)
        //endregion
        //region Merge output files
        Classifier.PostProcessStream()
        //endregion
    }
}
