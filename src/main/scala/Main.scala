import Session.Spark
import org.apache.spark.ml.feature.{NGram, RegexTokenizer, StopWordsRemover, Word2Vec}
import org.apache.spark.sql.functions.{udf, col}

object Main {
    def main(args: Array[String]): Unit = {
        // Create dataframe
        val df = Spark.createDataFrame(Seq(
            (0, "Hi! I heard about Spark"),
            (1, "I wish Java could use case classes"),
            (2, "Logistic regression models are neat"),
            (3, "Meh, I will drop"),
            (4, "Susan's paper-book is bad"),
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

        val countTokens = udf { (words: Seq[String]) => words.length }
        tokenized.select("sentence", "words")
          .withColumn("tokens", countTokens(col("words")))
          .show(false)
        // Filter words
        val filter = new StopWordsRemover()
          .setInputCol("words")
          .setOutputCol("filtered")
        val filtered = filter.transform(tokenized)

        filtered.select("words", "filtered")
          .show(false)
        // Take ngrams
        val ngram = new NGram()
          .setN(2)
          .setInputCol("filtered")
          .setOutputCol("ngrams")
        val ngrams = ngram.transform(filtered)

        ngrams.select("filtered", "ngrams")
          .show(false)
        // Vectorize
        val word2Vec = new Word2Vec()
          .setInputCol("ngrams")
          .setOutputCol("result")
          .setVectorSize(3)
          .setMinCount(0)
        val model = word2Vec.fit(ngrams)

        val result = model.transform(ngrams)
        result.select("ngrams", "result")
          .show(false)
    }
}
