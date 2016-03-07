package ladydi

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.{Pipeline, PipelineStage}

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

  def add (featureName : String , stages: List[PipelineStage]): Unit = {
    val first = stages.head
    inputSetter(featureName, first)

// chaining "stages" in an ML "PipelineStage" together
    for(s<-stages) {
      if(s != first) {
        inputSetter(featureName + n, s )
      }
      n += 1
      outputSetter(featureName + n, s)
    }
    featureStages =  stages :: featureStages
  }

  def add (featureNames : List[String] , stages: List[PipelineStage]): Unit = {
    val first = stages.head
    inputSetter(featureNames.toArray, first)

    // chaining "stages" in an ML "PipelineStage" together
    for(s<-stages) {
      if(s != first) {
        inputSetter(featureNames.toString + n, s)
      }
      n += 1
      outputSetter(featureNames.toString + n , s)
    }
    featureStages =  stages :: featureStages
  }



  def pipeline() : Pipeline = {
    val assembler = new VectorAssembler()
      .setInputCols(
        featureStages
          .map(_.last)
          .map(s => s.getOrDefault(s.getParam("outputCol")).toString)
          .toArray
      )
      .setOutputCol("features")

    new Pipeline().setStages((featureStages.flatten :+ assembler).toArray)
  }
}
