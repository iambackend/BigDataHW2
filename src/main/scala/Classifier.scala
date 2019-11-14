import Misc.CSV2DF
import Session.Spark
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification._
import org.apache.spark.ml.feature.{Word2VecModel, _}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.DataFrame


object Classifier {
    val ColTransformResult = "result"

    //region Classifiers

    private val lr = new LogisticRegression()
      .setRegParam(0.5)
    private val svc = new LinearSVC()
      .setRegParam(0.5)
    private val rf = new RandomForestClassifier()
      .setNumTrees(100)
    //endregion
    //region Transformations

    private val ColTokenizerResult = "words"
    private val ColFilterResult = "filtered"
    private val ColNGrams = "ngrams"
    private val ColVFiltered = "vfiltered"
    private val ColVNGrams = "vngrams"
    private val ColStrignDF = "sentence"

    private val tokenizer = new RegexTokenizer()
      .setOutputCol(ColTokenizerResult)
      //      .setPattern("(\\w+-\\w+)|\\w+")
      .setPattern("\\w+(-\\w+)?")
      .setGaps(false)
    private val filter = new StopWordsRemover()
      .setInputCol(ColTokenizerResult)
      .setOutputCol(ColFilterResult)
    private val ngram = new NGram()
      .setN(2)
      .setInputCol(ColFilterResult)
      .setOutputCol(ColNGrams)
    private val vectorizer = new Word2Vec()
      .setVectorSize(3)
      .setMinCount(0)
    private val assembler = new VectorAssembler()
      .setInputCols(Array(ColVFiltered, ColVNGrams))
      .setOutputCol(ColTransformResult)

    private var vWordsModel: Word2VecModel = _
    private var vNgramsModel: Word2VecModel = _
    //endregion
    //region Paths

    private val modelFolder = "models/"
    private val pathVWords = modelFolder + "vWords"
    private val pathVNgrams = modelFolder + "vNgrams"
    private val pathLR = modelFolder + "LR"
    private val pathSVC = modelFolder + "SVC"
    private val pathRF = modelFolder + "RF"
    //endregion

    private def extractFeatures(df: DataFrame, colInput: String): DataFrame = {
        tokenizer.setInputCol(colInput)
        ngram.transform(filter.transform(tokenizer.transform(df)))
    }

    private def features2Vector(df: DataFrame): DataFrame = {
        val vwords = vWordsModel.transform(df)
        val vngrams = vNgramsModel.transform(vwords)
        assembler.transform(vngrams)
    }

    def transformDataFrame(df: DataFrame, colInput: String): DataFrame = {
        features2Vector(extractFeatures(df, colInput))
    }

    def transformString(twit: String): DataFrame = {
        val df = Spark.createDataFrame(Seq(
            Tuple1(twit)
        )).toDF(ColStrignDF)
        transformDataFrame(df, ColStrignDF).select(ColTransformResult)
    }

    def init(df: DataFrame, colInput: String): Unit = {
        val features = extractFeatures(df, colInput)
        val load = Word2VecModel.load _
        // Set words2vector
        vectorizer.setInputCol(ColFilterResult).setOutputCol(ColVFiltered)
        vWordsModel = Model.FitOrLoad(vectorizer, features, load, pathVWords)
        // Set ngrams2vector
        vectorizer.setInputCol(ColNGrams).setOutputCol(ColVNGrams)
        vNgramsModel = Model.FitOrLoad(vectorizer, features, load, pathVNgrams)
    }

    private def trainModels(df: DataFrame, colLabels: String, colFeatures: String): Unit = {
        // Train logistic regression
        lr.setLabelCol(colLabels).setFeaturesCol(colFeatures)
        val lrmodel = Model.FitOrLoad(lr, df, LogisticRegressionModel.load, pathLR)
        // Train smv
        svc.setLabelCol(colLabels).setFeaturesCol(colFeatures)
        val scvmodel = Model.FitOrLoad(svc, df, LinearSVCModel.load, pathSVC)
        // Train random forest
        rf.setLabelCol(colLabels).setFeaturesCol(colFeatures)
        val rfmodel = Model.FitOrLoad(rf, df, RandomForestClassificationModel.load, pathRF)
    }

    def main(args: Array[String]): Unit = {
        //val df = Spark.createDataFrame(Seq(
        //    (0, "Hi! I heard about Spark"),
        //    (1, "I wish Java could use case classes"),
        //    (2, "Logistic regression models are neat"),
        //    (3, "Meh, I will drop"),
        //    (4, "Susan's paper-book is bad"),
        //    (5, "Co-operation is key to success"),
        //    (5, "Boss - a person who pay you money")
        //)).toDF("id", "sentence")

        val colText = "SentimentText"
        val colClass = "Sentiment"
        val df = CSV2DF("dataset/train.csv", headers = true).select(colClass, colText)
        //val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))
        init(df, colText)
        val data = transformDataFrame(df, colText).select(colClass, ColTransformResult)
        data.show(false)
        //trainModels(trainingData, colClass, colText)
        //Models.productIterator.foreach(x)
        //
        ////transformDataFrame(df, "sentence")
        //result.show(false)
        //transformString("Hi! I heard about Spark")
    }
}
