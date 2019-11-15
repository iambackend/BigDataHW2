import Session.Spark
import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.functions.{explode, desc}

object WordCount {
  def main(args: Array[String]): Unit = {
    // Create dataframe
    val df = Spark.createDataFrame(Seq(
      (0, "Hi! I heard about Spark"),
      (1, "I wish Java could use case classes"),
      (2, "Logistic regression models are neat"),
      (3, "Meh, I will drop drop"),
      (4, "Susan's paper-book is bad drop"),
      (5, "Co-operation is key to success"),
      (5, "Boss - a person who pay you money")
    )).toDF("id", "sentence")
    // Take words
    val tokenizer = new RegexTokenizer()
      .setInputCol("sentence")
      .setOutputCol("words")
      //          .setPattern("(\\w+-\\w+)|\\w+")
      .setPattern("\\w+(-\\w+)?")
      .setGaps(false)
    val tokenized = tokenizer.transform(df)

    // Filter words
    val filter = new StopWordsRemover()
      .setInputCol("words")
      .setOutputCol("filtered")
    val filtered = filter.transform(tokenized)

    filtered.select("words", "filtered")
      .show(false)



    val flatted = filtered.select(filtered("id"), explode(filtered("filtered")).as("word"))
    flatted.show(false)
    val count = flatted.distinct().groupBy("word").count().orderBy(desc("count"))
    count.show()
  }
}
