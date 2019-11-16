import Session.{HDFS, Spark}
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.spark.sql.{DataFrame, SaveMode}

object Misc {
    def CSV2DF(path: String, headers: Boolean = false, sep: String = ","): DataFrame = {
        Spark.read
          .option("header", headers)
          .option("sep", sep)
          .option("mode", "DROPMALFORMED")
          .csv(path)
    }

    def DF2CSV(df: DataFrame, path: String, mode: SaveMode = SaveMode.Overwrite,
               headers: Boolean = false, sep: String = ","): Unit = {
        df.write
          .mode(mode)
          .option("header", headers)
          .option("sep", sep)
          .csv(path)
    }

    def MergeFiles(pathin: Path, pathout: Path): Unit = {
        FileUtil.copyMerge(HDFS, pathin, HDFS, pathout, false, HDFS.getConf, null)
    }

    def MergeFiles(pathin: String, pathout: String): Unit = MergeFiles(new Path(pathin), new Path(pathout))

    def MergeFiles(pathin: Path, pathout: String): Unit = MergeFiles(pathin, new Path(pathout))

    def MergeFiles(pathin: String, pathout: Path): Unit = MergeFiles(new Path(pathin), pathout)

    def PathExists(path: Path): Boolean = HDFS.exists(path)

    def PathExists(path: String): Boolean = PathExists(new Path(path))

    def Remove(path: Path): Unit = HDFS.delete(path, true)

    def Remove(path: String): Unit = Remove(new Path(path))
}
