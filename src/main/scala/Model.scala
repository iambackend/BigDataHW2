import Misc.PathExists
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.ml.{Estimator, Model => SparkModel}
import org.apache.spark.sql.DataFrame

object Model {
    def Save[M <: MLWritable](model: M, p: String): Unit = {
        model.write.overwrite().save(p)
    }

    def FitOrLoad[M <: SparkModel[M] with MLWritable, F <: Estimator[M]]
    (fitter: F, df: DataFrame, load: (String) => M, path: String): M = {
        if (PathExists(path)) return load(path)
        val vWordsModel = fitter.fit(df)
        Save(vWordsModel, path)
        vWordsModel
    }
}
