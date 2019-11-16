import Misc.Remove
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{current_timestamp, date_format}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}

object Stream {
    private val Host = "10.90.138.32"
    private val Port = 8989

    val BatchDFColText = "sentences"
    val BachDFColDateTime = "datetime"
    val OutputDir = "output/"

    val CheckPointDir = "tmp"
    private val QueryName = "twitter"
    private var Query: StreamingQuery = _
    private var InputThread: Thread = _

    def start(timeout: Long): Unit = {
        //region Initialization
        import Session.Spark
        import Spark.implicits._
        Classifier.Init()
        WordCounter.Init()
        InputThread = new Thread {
            override def run() {
                while (true) {
                    print("Type 'stop' to stop stream: ")
                    val line = scala.io.StdIn.readLine().trim.toLowerCase
                    if (line == "stop") {
                        Query.stop()
                        return
                    }
                }
            }
        }
        InputThread.setDaemon(true)
        //endregion
        //region Start stream

        val data = Spark.readStream
          .format("socket")
          .option("host", Host)
          .option("port", Port)
          .load()
        Remove(CheckPointDir)
        Query = data.as[String].toDF(BatchDFColText)
          .withColumn(BachDFColDateTime, date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss"))
          .writeStream
          .foreachBatch {
              (batchDF: DataFrame, batchId: Long) =>
                  if (batchDF.count() > 0) {
                      try {
                          WordCounter.ProcessStream(batchDF)
                          Classifier.ProcessStream(batchDF)
                      } catch {
                          case e: Exception =>
                              batchDF.show(false)
                              e.printStackTrace()
                      }
                  }
          }
          .outputMode(OutputMode.Append())
          .queryName(QueryName)
          .option("checkpointLocation", CheckPointDir)
          .start()
        //endregion
        //region Start input thread and await query termination

        InputThread.start()
        if (timeout > 0) Query.awaitTermination(timeout) else Query.awaitTermination()
        Query.stop()
        //endregion
        //region Postprocessing
        WordCounter.PostProcessStream()
        Classifier.PostProcessStream()
        Remove(CheckPointDir)
        //endregion
    }
}
