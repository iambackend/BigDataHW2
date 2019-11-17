import FileSystem._
import Session.Spark.implicits._
import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object WordCounter {
    private val OldResult = Stream.OutputDir + "WordCounterOld.csv"
    private val OutputCSV = Stream.OutputDir + "WordCounter.csv"
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

    private def Load(path: String = OutputCSV): DataFrame = {
        CSV2DF(path, headers = true, sep = Separator)
    }

    private def Save(df: DataFrame = wordcount, path: String = OutputCSV): Unit = {
        DF2CSVFile(df, path, headers = true, sep = Separator)
    }

    def Init(): Unit = {
        if (PathExists(OutputCSV)) {
            val temp = Load()
            if (temp.count() > 0) {
                // Save old data to temporary dir and load from there
                // Otherwise function Save will overwrite (i. e. delete) using files
                // and an error will be thrown
                Save(temp, OldResult)
                wordcount = Load(OldResult)
                wordcount = wordcount.withColumn(ColCount, wordcount(ColCount).cast(IntegerType))
            }
        }
    }

    def ProcessStream(batchDF: DataFrame): Unit = {
        val words = GetWords(batchDF)
          .flatMap(r => r.getSeq[String](0).distinct)
          .withColumnRenamed("value", ColWord)
          .withColumn(ColCount, typedLit[Int](1))
        wordcount = wordcount
          .union(words)
          .groupBy(ColWord)
          .agg(sum(ColCount).as(ColCount))
          .orderBy(desc(ColCount))
        Save()
    }

    def PostProcessStream(): Unit = {
        Remove(OldResult)
    }
}
