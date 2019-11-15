import Classifier.{ColPredClass, ColTrueClass}
import org.apache.spark.sql.DataFrame

object Scores {
    type LabelType = Double

    def CountCorrect(results: DataFrame): Long = {
        results.where(s"$ColTrueClass == $ColPredClass").count()
    }

    def CountCorrectForLabel(results: DataFrame, label: LabelType): Long = {
        results.where(s"$ColPredClass == $label and $ColTrueClass == $label").count()
    }

    def CountTrueForLabel(results: DataFrame, label: LabelType): Long = {
        results.where(s"$ColTrueClass == $label").count()
    }

    def CountPredForLabel(results: DataFrame, label: LabelType): Long = {
        results.where(s"$ColPredClass == $label").count()
    }

    def accuracy(results: DataFrame): Double = {
        100.0 * CountCorrect(results) / results.count()
    }

    def precision(results: DataFrame, label: LabelType): Double = {
        100.0 * CountCorrectForLabel(results, label) / CountPredForLabel(results, label)
    }

    def recall(results: DataFrame, label: LabelType): Double = {
        100.0 * CountCorrectForLabel(results, label) / CountTrueForLabel(results, label)
    }

    def FScoreTuple(results: DataFrame, label: LabelType, beta: Double = 1.0): (Double, Double, Double) = {
        val correct = CountCorrectForLabel(results, label).toDouble
        val precision = correct / CountPredForLabel(results, label)
        val recall = correct / CountTrueForLabel(results, label)
        val b = beta * beta
        (precision * 100.0, recall * 100.0,
          (1.0 + b) * precision * recall / (b * precision + recall))
    }

    def FScore(results: DataFrame, label: LabelType, beta: Double = 1.0): Double = {
        FScoreTuple(results, label, beta)._3
    }

}
