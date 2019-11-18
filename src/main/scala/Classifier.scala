import FileSystem._
import Scores.{FScoreTuple, LabelType, accuracy}
import org.apache.spark.ml.classification._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SaveMode}


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
    private var Models: Array[Model] = _
    private var Labels: Array[LabelType] = _
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

    private def TransformDataFrame(df: DataFrame): DataFrame = PipeModel.transform(df)

    private def ConvertInput(df: DataFrame, colText: String, colClass: String = null): DataFrame = {
        var data = df
        if (colClass != null) {
            val changetype = df.select(colClass).dtypes(0)._2 != DoubleType.toString
            if (changetype)
                data = df.withColumn(ColTrueClass, df(colClass).cast(DoubleType)).drop(colClass)
            else if (colClass != ColTrueClass)
                data = df.withColumnRenamed(colClass, ColTrueClass).drop(colClass)
        }
        if (colText != ColPipeInput)
            data = data.withColumnRenamed(colText, ColPipeInput).drop(colText)
        data
    }

    private def trainModels(data: DataFrame): Unit = {
        Models = Array(
            Model.FitOrLoadModel(rf, data, RandomForestClassificationModel.load, pathRF),
            Model.FitOrLoadModel(lr, data, LogisticRegressionModel.load, pathLR),
            Model.FitOrLoadModel(ann, data, MultilayerPerceptronClassificationModel.load, pathANN)
            //Model.FitOrLoadModel(svc, data, LinearSVCModel.load, pathSVC),
            //Model.FitOrLoadModel(nb, data, NaiveBayesModel.load, pathNB)
        )
    }

    private def printResults(modelName: String, results: DataFrame): Unit = {
        println("\n" + modelName)
        println(f"accuracy: ${accuracy(results)}%.2f%%")
        Labels.foreach { label =>
            println(s"label $label")
            val (pre, re, f1) = FScoreTuple(results, label)
            println(f"    precision: $pre%.2f%%")
            println(f"    recall: $re%.2f%%")
            println(f"    F1 score: $f1%.4f")
        }
    }

    def Init(): Unit = {
        // Get train data
        val colText = "SentimentText"
        val colClass = "Sentiment"
        var train = CSV2DF("dataset/train.csv", headers = true).select(colClass, colText)
        train = ConvertInput(train, colText, colClass)
        // Get labels
        Labels = train.select(ColTrueClass).distinct().collect().map(_.getDouble(0)).sorted
        // Print available labels
        Labels.foreach { l =>
            s"Label $l, ${train.where(s"$ColTrueClass == $l").count()} sample(s)"
        }
        // Fit pipeline
        PipeModel = Model.FitOrLoad(pipeline, train, PipelineModel.load, pathPipe)
        // Transform train data
        val data = TransformDataFrame(train).select(ColTrueClass, ColVectors)
        // Fit models
        trainModels(data)
    }

    def ProcessStream(batchDF: DataFrame): Unit = {
        val data = TransformDataFrame(ConvertInput(batchDF, Stream.BatchDFColText))
          .select(Stream.BachDFColDateTime, ColPipeInput, ColVectors)
        Models.foreach { m =>
            // Get predictions
            val result = m.transform(data).select(Stream.BachDFColDateTime, ColPipeInput, ColPredClass)
            // Write time, sentence, predicted class
            DF2CSV(result, m.outdir, mode = SaveMode.Append)
        }
    }

    def PostProcessStream(): Unit = {
        Models.foreach { m =>
            MergeFiles(m.outdirPath, m.outcsvPath)
        }
    }

    //def main(args: Array[String]): Unit = {}
}
