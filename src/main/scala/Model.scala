import Misc.PathExists
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.ml.{Estimator, Model => SparkModel}
import org.apache.spark.sql.DataFrame

object Model {
    def Save[M <: MLWritable](model: M, path: String): Unit = {
        model.write.overwrite().save(path)
    }

    def FitOrLoad[M <: SparkModel[M] with MLWritable, F <: Estimator[M]]
    (fitter: F, df: DataFrame, load: String => M, path: String): M = {
        if (PathExists(path)) return load(path)
        val model = fitter.fit(df)
        Save(model, path)
        model
    }
}
