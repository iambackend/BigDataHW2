import FileSystem.{CreateFile, DF2CSVFile, Remove}
import Session.Spark
import Session.Spark.implicits._
import org.apache.commons.lang.exception.ExceptionUtils.getStackTrace
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{current_timestamp, date_format}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}

object Stream {
    private val Host = "10.90.138.32"
    private val Port = 8989

    val BatchDFColText = "sentences"
    val BachDFColDateTime = "datetime"
    val OutputDir = "output/"

    private val CheckPointDir = "tmp"
    private val ErrorDir = "errors/"
    private var ErrorCounter = 0

    private val QueryName = "twitter"
    private var Query: StreamingQuery = _

    private def BatchReceived(batchDF: DataFrame, batchId: Long) {
        if (batchDF.count() > 0) {
            try {
                WordCounter.ProcessStream(batchDF)
                Classifier.ProcessStream(batchDF)
            } catch {
                case e: Exception =>
                    val path = ErrorDir + s"err$ErrorCounter/"
                    CreateFile(path + "trace.txt", getStackTrace(e))
                    DF2CSVFile(batchDF, path + "tweets.csv")
                    ErrorCounter += 1
            }
        }
    }

    def start(timeout: Long): Unit = {
        //region Initialization
        Remove(CheckPointDir)
        Remove(ErrorDir)
        Classifier.Init()
        //endregion
        //region Start stream

        val data = Spark.readStream
          .format("socket")
          .option("host", Host)
          .option("port", Port)
          .load()
        Query = data.as[String].toDF(BatchDFColText)
          .withColumn(BachDFColDateTime, date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
          .writeStream
          .foreachBatch(BatchReceived _)
          .outputMode(OutputMode.Append())
          .queryName(QueryName)
          .option("checkpointLocation", CheckPointDir)
          .start()
        //endregion
        //region Await query termination
        Query.awaitTermination(timeout)
        Query.stop()
        //endregion
        //region Postprocessing
        WordCounter.PostProcessStream()
        Classifier.PostProcessStream()
        Remove(CheckPointDir)
        //endregion
    }
}
