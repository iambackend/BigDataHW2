import Session.Spark
import org.apache.hadoop.fs.{FileUtil, Path, FileSystem => FS}
import org.apache.spark.sql.{DataFrame, SaveMode}

object FileSystem {
    private val HDFS: FS = FS.get(Spark.sparkContext.hadoopConfiguration)

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

    def PathExists(path: Path): Boolean = HDFS.exists(path)

    def PathExists(path: String): Boolean = PathExists(new Path(path))

    def HasFiles(path: Path): Boolean = HDFS.getContentSummary(path).getFileCount > 0

    def HasFiles(path: String): Boolean = HasFiles(new Path(path))

    def CreateDirs(path: Path): Unit = HDFS.mkdirs(path)

    def CreateDirs(path: String): Unit = HDFS.mkdirs(new Path(path))

    def CreateFile(path: Path, content: String): Unit = {
        val out = HDFS.create(path, true)
        out.writeBytes(content)
        out.close()
    }

    def DF2CSVFile(df: DataFrame, path: String, headers: Boolean = false, sep: String = ","): Unit = {
        var text = df.collect().map(a => a.mkString(sep)).mkString("\n") + "\n"
        if (headers) text = df.columns.mkString(sep) + "\n" + text
        CreateFile(path, text)
    }

    def CreateFile(path: String, content: String): Unit = CreateFile(new Path(path), content)

    def Remove(path: Path): Unit = if (PathExists(path)) HDFS.delete(path, true)

    def Remove(path: String): Unit = Remove(new Path(path))

    def MergeFiles(pathin: Path, pathout: Path): Unit = {
        Remove(pathout)
        FileUtil.copyMerge(HDFS, pathin, HDFS, pathout, false, HDFS.getConf, null)
    }

    def MergeFiles(pathin: String, pathout: String): Unit = MergeFiles(new Path(pathin), new Path(pathout))

    def MergeFiles(pathin: Path, pathout: String): Unit = MergeFiles(pathin, new Path(pathout))

    def MergeFiles(pathin: String, pathout: Path): Unit = MergeFiles(new Path(pathin), pathout)
}
