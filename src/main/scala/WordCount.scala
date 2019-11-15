import Session.Spark
import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import Spark.implicits._
import Misc._

object WordCount {
  def path = "dataset/words.csv"
  var df: DataFrame = Seq.empty[(String, Int)].toDF("word", "count")
  if(PathExists(path))
    df = CSV2DF(path, headers = true)

  def main(args: Array[String]): Unit = {
    df = Spark.createDataFrame(Seq(
      ("dank", 12),
      ("drop", 123),
      ("wish", 2)
    )).toDF("word", "count")

    val input = Spark.createDataFrame(Seq(
      (0, "Hi! I heard about Spark"),
      (1, "I wish Java could use case classes"),
      (2, "Logistic regression models are neat"),
      (3, "Meh, I will drop drop"),
      (4, "Susan's paper-book is bad drop"),
      (5, "Co-operation is key to success"),
      (6, "Boss - a person who pay you money")
    )).toDF("id", "sentence")
    feed_twits(input)
  }


  def init_empty(): Unit = {
    df = Seq.empty[(String, Int)].toDF("word", "count")
  }

  def write(): Unit = {
    df = df.orderBy(desc("count"))
    DF2CSV(path, headers = true, df)
  }

  def feed_twits(twits: DataFrame): Unit = {
    // Take words
    val tokenizer = new RegexTokenizer()
      .setInputCol("sentence")
      .setOutputCol("words")
      //          .setPattern("(\\w+-\\w+)|\\w+")
      .setPattern("\\w+(-\\w+)?")
      .setGaps(false)
    val tokenized = tokenizer.transform(twits)

    // Filter words
    val filter = new StopWordsRemover()
      .setInputCol("words")
      .setOutputCol("filtered")
    val filtered = filter.transform(tokenized)

//    filtered.select("words", "filtered")
//      .show(false)



    val flatted = filtered.select(filtered("id"), explode(filtered("filtered")).as("word"))
//    flatted.show(false)
    val count = flatted.distinct().groupBy("word").count()
//    count.orderBy(desc("count")).show()

    df = df.as('a).join(count.as('b), $"a.word" === $"b.word","full")
      .select(coalesce($"a.word", $"b.word").alias("word"), $"a.count".alias("x"), $"b.count".alias("y"))
      .na.fill(0)
      .select('word, ('x + 'y).alias("count"))

    df.orderBy(desc("count")).show()
  }
}
