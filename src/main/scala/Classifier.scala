import Misc.CSV2DF
import Scores.{FScoreTuple, LabelType, accuracy}
import Session.Spark
import org.apache.spark.ml.classification._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.{Pipeline, PipelineModel, Transformer}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DoubleType


object Classifier {
    //region Columns

    private val ColPipeInput = "sentences"
    private val ColTokenizerResult = "words"
    private val ColFilterResult = "filtered"
    private val ColNGrams = "ngrams"
    private val ColVFiltered = "vfiltered"
    private val ColVNGrams = "vngrams"
    private val ColVNormFiltered = "vnormfiltered"
    private val ColVNormNGrams = "vnormngrams"

    val ColVectors = "vectors" // do not change!
    val ColTrueClass = "label" // do not change!
    val ColPredClass = "prediction" // do not change!
    //endregion
    //region Transformations

    private val tokenizer = new RegexTokenizer()
      .setInputCol(ColPipeInput)
      .setOutputCol(ColTokenizerResult)
      //.setPattern("(\\w+-\\w+)|\\w+")
      .setPattern("\\w+(-\\w+)?")
      .setGaps(false)
    private val filter = new StopWordsRemover()
      .setInputCol(ColTokenizerResult)
      .setOutputCol(ColFilterResult)
    private val ngrams = new NGram()
      .setN(2)
      .setInputCol(ColTokenizerResult)
      .setOutputCol(ColNGrams)
    private val words2vec = new Word2Vec()
      .setInputCol(ColFilterResult)
      .setOutputCol(ColVFiltered)
      .setVectorSize(3)
      .setMinCount(0)
    private val ngrams2vec = new Word2Vec()
      .setInputCol(ColNGrams)
      .setOutputCol(ColVNGrams)
      .setVectorSize(6)
      .setMinCount(0)
    private val vwordsnorm = new MinMaxScaler()
      .setInputCol(ColVFiltered)
      .setOutputCol(ColVNormFiltered)
    private val vngramsnorm = new MinMaxScaler()
      .setInputCol(ColVNGrams)
      .setOutputCol(ColVNormNGrams)
    private val assembler = new VectorAssembler()
      .setInputCols(Array(ColVNormFiltered, ColVNormNGrams))
      .setOutputCol(ColVectors)
    private val pipeline = new Pipeline()
      .setStages(Array(tokenizer, filter, ngrams, words2vec, ngrams2vec, vwordsnorm, vngramsnorm, assembler))
    private var PipeModel: PipelineModel = _
    //endregion
    //region Classifiers

    private val rf = new RandomForestClassifier()
      .setLabelCol(ColTrueClass)
      .setFeaturesCol(ColVectors)
      .setPredictionCol(ColPredClass)
      .setNumTrees(200)
    private val lr = new LogisticRegression()
      .setLabelCol(ColTrueClass)
      .setFeaturesCol(ColVectors)
      .setPredictionCol(ColPredClass)
    private val ann = new MultilayerPerceptronClassifier()
      .setLabelCol(ColTrueClass)
      .setFeaturesCol(ColVectors)
      .setPredictionCol(ColPredClass)
      .setLayers(Array(9, 18, 36, 6, 2))
    private val svc = new LinearSVC()
      .setLabelCol(ColTrueClass)
      .setFeaturesCol(ColVectors)
      .setPredictionCol(ColPredClass)
      .setRegParam(1.0)
      .setTol(1E-3)
    private val nb = new NaiveBayes()
      .setLabelCol(ColTrueClass)
      .setFeaturesCol(ColVectors)
      .setPredictionCol(ColPredClass)
    private var Models: Array[Transformer] = _
    //endregion
    //region Paths

    private val modelFolder = "models/"
    private val pathPipe = modelFolder + "Pipe"
    private val pathRF = modelFolder + "RF"
    private val pathLR = modelFolder + "LR"
    private val pathANN = modelFolder + "ANN"
    private val pathSVC = modelFolder + "SVC"
    private val pathNB = modelFolder + "NB"
    //endregion

    def TransformDataFrame(df: DataFrame): DataFrame = {
        PipeModel.transform(df)
    }

    def TransformString(twit: String): DataFrame = {
        val df = Spark.createDataFrame(Seq(
            Tuple1(twit)
        )).toDF(ColPipeInput)
        TransformDataFrame(df).select(ColVectors)
    }

    def ConvertInput(df: DataFrame, colClass: String, colText: String): DataFrame = {
        val changetype = df.select(colClass).dtypes(0)._2 != DoubleType.toString
        var data =
            if (changetype)
                df.withColumn(ColTrueClass, df(colClass).cast(DoubleType)).drop(colClass)
            else if (colClass != ColTrueClass)
                df.withColumnRenamed(colClass, ColTrueClass).drop(colClass)
            else
                df
        if (colText != ColPipeInput)
            data = data.withColumnRenamed(colText, ColPipeInput).drop(colText)
        data
    }

    private def trainModels(data: DataFrame): Unit = {
        Models = Array(
            Model.FitOrLoad(rf, data, RandomForestClassificationModel.load, pathRF),
            Model.FitOrLoad(lr, data, LogisticRegressionModel.load, pathLR),
            Model.FitOrLoad(ann, data, MultilayerPerceptronClassificationModel.load, pathANN),
            Model.FitOrLoad(svc, data, LinearSVCModel.load, pathSVC),
            Model.FitOrLoad(nb, data, NaiveBayesModel.load, pathNB)
        )
    }

    private def loadModels(): Unit = {
        Models = Array(
            RandomForestClassificationModel.load(pathRF),
            LogisticRegressionModel.load(pathLR),
            MultilayerPerceptronClassificationModel.load(pathANN),
            LinearSVCModel.load(pathSVC),
            NaiveBayesModel.load(pathNB)
        )
    }

    def train(df: DataFrame, colClass: String, colText: String): Unit = {
        // Convert input
        val class_text = ConvertInput(df, colClass: String, colText: String)
        // Fit pipeline
        PipeModel = Model.FitOrLoad(pipeline, class_text, PipelineModel.load, pathPipe)
        // Transform train data
        val data = TransformDataFrame(class_text).select(ColTrueClass, ColVectors)
        // Fit models
        trainModels(data)
    }

    def printResults(modelName: String, results: DataFrame, labels: Array[LabelType]): Unit = {
        println("\n" + modelName)
        println(f"accuracy: ${accuracy(results)}%.2f%%")
        labels.foreach { label =>
            println(s"label $label")
            val (pre, re, f1) = FScoreTuple(results, label)
            println(f"    precision: $pre%.2f%%")
            println(f"    recall: $re%.2f%%")
            println(f"    F1 score: $f1%.4f")
        }
    }

    def test(df: DataFrame, colClass: String, colText: String): Unit = {
        // Pipeline
        PipeModel = PipelineModel.load(pathPipe)
        // Models
        loadModels()
        // Convert test data
        val class_text = ConvertInput(df, colClass: String, colText: String)
        // Get available labels
        val labels = class_text.select(ColTrueClass).distinct().collect().map(_.getDouble(0)).sorted
        // Transform test data
        val test = TransformDataFrame(class_text).select(ColTrueClass, ColVectors)
        // Print results
        Models.foreach(
            //m => m.transform(test).select("rawPrediction", ColPredClass, ColTrueClass).show(false)
            m => printResults(m.getClass.toString, m.transform(test), labels)
        )
    }

    def main(args: Array[String]): Unit = {
        //val df = Spark.createDataFrame(Seq(
        //    (0, "Hi! I heard about Spark"),
        //    (1, "I wish Java could use case classes"),
        //    (2, "Logistic regression models are neat"),
        //    (3, "Meh, I will drop"),
        //    (4, "Susan's paper-book is bad"),
        //    (5, "Co-operation is key to success"),
        //    (6, "Boss - a person who pay you money")
        //)).toDF("id", "sentence")
        val colText = "SentimentText"
        val colClass = "Sentiment"
        val df = CSV2DF("dataset/train.csv", headers = true).select(colClass, colText)
        train(df, colClass, colText)
        test(df, colClass, colText)
    }
}
