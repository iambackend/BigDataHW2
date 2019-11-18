import FileSystem._
import Session.Spark.implicits._
import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SaveMode}

object WordCounter {
    private val OutputDir = Stream.OutputDir + "WordCounter"
    private val OutputCSV = OutputDir + ".csv"
    private val Separator = "\t"
    private val SaveHeaders = true

    private val ColTokenizer = "words"
    private val ColFilter = "filtered"
    private val ColWord = "word"
    private val ColCount = "count"

    private val tokenizer = new RegexTokenizer()
      .setInputCol(Stream.BatchDFColText)
      .setOutputCol(ColTokenizer)
      .setMinTokenLength(3)
      .setPattern("\\w+(-\\w+)?")
      .setGaps(false)
    private val filter = new StopWordsRemover()
      .setInputCol(ColTokenizer)
      .setOutputCol(ColFilter)

    private def GetWords(df: DataFrame): DataFrame = {
        filter.transform(tokenizer.transform(df)).select(ColFilter)
    }

    private def Load(path: String): DataFrame = {
        CSV2DF(path, SaveHeaders, Separator)
    }

    private def Save(df: DataFrame, path: String): Unit = {
        DF2CSV(df, path, SaveMode.Append, SaveHeaders, Separator)
    }

    def ProcessStream(batchDF: DataFrame): Unit = {
        val words = GetWords(batchDF)
          .flatMap(r => r.getSeq[String](0).distinct)
          .withColumnRenamed("value", ColWord)
          .withColumn(ColCount, typedLit[Int](1))
        Save(words, OutputDir)
    }

    def PostProcessStream(): Unit = {
        var sorted = Load(OutputDir)
        val changetype = sorted.select(ColCount).dtypes(0)._2 != IntegerType.toString
        if (changetype) sorted = sorted.withColumn(ColCount, sorted(ColCount).cast(IntegerType))
        sorted = sorted
          .groupBy(ColWord)
          .agg(sum(ColCount).as(ColCount))
          .orderBy(desc(ColCount))
        DF2CSVFile(sorted, OutputCSV, SaveHeaders, Separator)
    }
}
