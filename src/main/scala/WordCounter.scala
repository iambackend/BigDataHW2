import FileSystem._
import Session.Spark.implicits._
import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object WordCounter {
    private val OutputDir = Stream.OutputDir + "WordCounter"
    private val TempDir = Stream.OutputDir + "temp"
    private val OutputCSV = OutputDir + ".csv"
    private val Separator = "\t"

    private val ColTokenizer = "words"
    private val ColFilter = "filtered"
    private val ColWord = "word"
    private val ColCount = "count"

    private val tokenizer = new RegexTokenizer()
      .setInputCol(Stream.BatchDFColText)
      .setOutputCol(ColTokenizer)
      .setPattern("\\w+(-\\w+)?")
      .setGaps(false)
    private val filter = new StopWordsRemover()
      .setInputCol(ColTokenizer)
      .setOutputCol(ColFilter)

    private var wordcount: DataFrame = Seq.empty[(String, Int)].toDF(ColWord, ColCount)

    private def GetWords(df: DataFrame): DataFrame = {
        filter.transform(tokenizer.transform(df)).select(ColFilter)
    }

    private def Load(path: String = OutputDir): DataFrame = {
        CSV2DF(path, headers = true, sep = Separator)
    }

    private def Save(df: DataFrame = wordcount, path: String = OutputDir): Unit = {
        DF2CSV(df, path, headers = true, sep = Separator)
    }

    def Init(): Unit = {
        if (PathExists(OutputDir) && HasFiles(OutputDir)) {
            val temp = Load()
            if (temp.count() > 0) {
                // Save old data to temporary dir and load from there
                // Otherwise function Save will overwrite (i. e. delete) using files
                // and an error will be thrown
                Save(temp, TempDir)
                wordcount = Load(TempDir)
                wordcount = wordcount.withColumn(ColCount, wordcount(ColCount).cast(IntegerType))
            }
        }
    }

    def ProcessStream(batchDF: DataFrame): Unit = {
        val words = GetWords(batchDF)
          .flatMap(r => r.getSeq[String](0))
          .withColumnRenamed("value", ColWord)
          .withColumn(ColCount, typedLit[Int](1))
        wordcount = wordcount
          .union(words)
          .groupBy(ColWord)
          .agg(sum(ColCount).as(ColCount))
          .orderBy(desc(ColCount))
          .coalesce(1)
        Save()
    }

    def PostProcessStream(): Unit = {
        MergeFiles(OutputDir, OutputCSV)
        Remove(TempDir)
    }
}
