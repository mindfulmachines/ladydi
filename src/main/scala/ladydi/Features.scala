package ladydi

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

  private def chain(featureName : String , stages: List[PipelineStage]) = {
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
        inputSetter(featureNames.toString + n, s)
      }
      n += 1
      outputSetter(featureNames.toString + n, s)
    }
    stages
  }

  def add (featureNames : List[String] , stages: List[PipelineStage]): Unit = {
    featureStages =  chain(featureNames, stages) :: featureStages

  }


  def pipeline(output: String, stages: List[PipelineStage] = Nil) : Pipeline = {
    if(stages.isEmpty) {
      val noChain = featureStages.flatten
      outputSetter(output, noChain.last)
      new Pipeline().setStages(noChain.toArray)
    } else {
      val chained = chain(
        featureStages
          .map(_.last)
          .map(s => s.getOrDefault(s.getParam("outputCol")).toString)
        , stages
      )
      outputSetter(output, chained.last)

      new Pipeline().setStages((featureStages.flatten ::: chained).toArray)
    }

  }
}
