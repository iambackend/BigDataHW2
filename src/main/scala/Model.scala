import FileSystem.PathExists
import org.apache.hadoop.fs.Path
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.ml.{Estimator, Transformer, Model => SparkModel}
import org.apache.spark.sql.DataFrame

object Model {
    private val OutputDir = Stream.OutputDir

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

    def FitOrLoadModel[M <: SparkModel[M] with MLWritable, F <: Estimator[M]]
    (fitter: F, df: DataFrame, load: String => M, path: String): Model = {
        Model(FitOrLoad(fitter, df, load, path))
    }
}

case class Model(model: Transformer) {
    val name: String = model.getClass.getSimpleName
    val outdir: String = Model.OutputDir + name
    val outcsv: String = outdir + ".csv"
    val outdirPath: Path = new Path(outdir)
    val outcsvPath: Path = new Path(outcsv)

    def transform(dataFrame: DataFrame): DataFrame = {
        model.transform(dataFrame)
    }
}
