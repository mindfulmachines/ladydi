package org.apache.spark.ml

import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ListBuffer
import scala.collection.parallel.ForkJoinTaskSupport


/**
  * Created by zafshar on 3/30/16.
  */
object Fitting {
  /**
    * Fits the pipeline to the input dataset with additional parameters. If a stage is an
    * [[Estimator]], its [[Estimator#fit]] method will be called on the input dataset to fit a model.
    * Then the model, which is a transformer, will be used to transform the dataset as the input to
    * the next stage. If a stage is a [[Transformer]], its [[Transformer#transform]] method will be
    * called to produce the dataset for the next stage. The fitted model from a [-[-Pipeline]] is an
    * [[PipelineModel]], which consists of fitted models and transformers, corresponding to the
    * pipeline stages. If there are no stages, the output model acts as an identity transformer.
    *
    * @param dataset input dataset
    * @return fitted pipeline
    */
   def fit(dataset: DataFrame, theStages: Array[PipelineStage]): Array[Transformer] = {
    // Search for the last estimator.
    var indexOfLastEstimator = -1
    theStages.view.zipWithIndex.foreach { case (stage, index) =>
      stage match {
        case _: Estimator[_] =>
          indexOfLastEstimator = index
        case _ =>
      }
    }
    var curDataset = dataset
    val transformers = ListBuffer.empty[Transformer]
    theStages.view.zipWithIndex.foreach { case (stage, index) =>
      if (index <= indexOfLastEstimator) {
        val transformer = stage match {
          case estimator: Estimator[_] =>
            estimator.fit(curDataset)
          case t: Transformer =>
            t
          case _ =>
            throw new IllegalArgumentException(
              s"Do not support stage $stage of type ${stage.getClass}")
        }
        if (index < indexOfLastEstimator) {
          curDataset = transformer.transform(curDataset)
        }
        transformers += transformer
      } else {
        transformers += stage.asInstanceOf[Transformer]
      }
    }
    transformers.toArray
  }
  def fit (df : DataFrame, featureStages: List[List[PipelineStage]]): PipelineModel = {
    df.cache().count()
    val uid = Identifiable.randomUID("pipeline")
    val transformers = featureStages.par.map(
        f => Fitting.fit(df,f.toArray)
      ).flatten.toArray
    new PipelineModel(uid, transformers)
  }
}
