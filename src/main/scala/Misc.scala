import Session.{HDFS, Spark}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame

object Misc {
    def CSV2DF(path: String, headers: Boolean): DataFrame = {
        Spark.read
          .format("csv")
          .option("header", headers.toString)
          .option("mode", "DROPMALFORMED")
          .load(path)
    }

    def DF2CSV(path: String, headers: Boolean, df: DataFrame): Unit = {
        df.write
          .format("csv")
          .option("header", headers.toString)
          .option("mode", "DROPMALFORMED")
          .save(path)
    }

    def PathExists(path: String): Boolean = {
        HDFS.exists(new Path(path))
    }
}
