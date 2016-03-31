package ladydi

import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{PipelineModel, Pipeline, PipelineStage}
import org.apache.spark.sql.DataFrame

/**
  * Created by zafshar on 3/6/16.
  */
class Features {
  var featureStages : List[List[PipelineStage]] = Nil
  var n: Int = 0
  val inputColumnNames  = "inputCol" ::
                          "inputCols" ::
                          "featuresCol" :: Nil
  val outputColumnNames = "outputCol" ::
                          "topicDistributionCol" :: Nil

  def inputSetter[T] (input: T, stage: PipelineStage): Unit ={
    val inputCol = inputColumnNames.filter(stage.hasParam).head
    stage.set(
      stage.getParam(inputCol),
      input)
  }
  def outputSetter (output: String, stage: PipelineStage): Unit ={
    val outputCol = outputColumnNames.filter(stage.hasParam).head
    stage.set(
      stage.getParam(outputCol),
      output)
  }

  private def chain(featureName : String , stages: List[PipelineStage]) = {
    val first = stages.head
    inputSetter(featureName, first)

// chaining "stages" in an ML "PipelineStage" together
    for(s<-stages) {
      if(s != first) {
        inputSetter("ladydi_" + featureName + n, s )
      }
      n += 1
      outputSetter("ladydi_" +featureName + n, s)
    }
    stages
  }

  def add(featureName : String , stages: List[PipelineStage]): Unit = {
    featureStages =  chain(featureName, stages) :: featureStages
  }

  private def chain (featureNames : List[String] , stages: List[PipelineStage]) = {
    val first = stages.head
    inputSetter(featureNames.toArray, first)

    // chaining "stages" in an ML "PipelineStage" together
    for (s <- stages) {
      if (s != first) {
        inputSetter("ladydi_" +featureNames.toString + n, s)
      }
      n += 1
      outputSetter("ladydi_" +featureNames.toString + n, s)
    }
    stages
  }

  def add (featureNames : List[String] , stages: List[PipelineStage]): Unit = {
    featureStages =  chain(featureNames, stages) :: featureStages

  }


  def pipeline(output: String, stages: List[PipelineStage], featureSubset: List[String]) : Pipeline = {
    if(stages.isEmpty) {
      val noChain = featureStages.flatten
      outputSetter(output, noChain.last)
      new Pipeline().setStages(noChain.toArray)
    } else {
      val chained = chain(
        featureStages
          .filter(s => featureSubset.contains(s.head.getOrDefault(s.head.getParam("inputCol")).toString) )
          .map(_.last)
          .map(s => s.getOrDefault(s.getParam("outputCol")).toString)
        , stages
      )
      outputSetter(output, chained.last)

      new Pipeline().setStages((featureStages.flatten ::: chained).toArray)
    }

  }

  def pipeline(output: String, stages: List[PipelineStage] = Nil) : Pipeline = {
    pipeline(
      output,
      stages,
      featureStages.map(s => s.head.getOrDefault(s.head.getParam("inputCol")).toString)
    )

  }

  def pipeline() : Pipeline = {
    val noChain = featureStages.flatten
    new Pipeline().setStages(noChain.toArray)

  }

  def fit (df : DataFrame): PipelineModel = {
    val uid = Identifiable.randomUID("pipeline")
    val transformers = featureStages.flatten
    featureStages.par.foreach(
      f => Fitting.fit(df,f.toArray)
    )
    new PipelineModel(uid, transformers.toArray)
  }


}
