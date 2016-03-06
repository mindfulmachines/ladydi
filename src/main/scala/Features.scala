import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.param.Param

/**
  * Created by zafshar on 3/6/16.
  */
class Features {
  var featureStages : List[List[PipelineStage]] = Nil
  var n: Int = 0
  def add (input : String , stages: List[PipelineStage]): Unit = {
    val first = stages.head
    first.set(
      first.getParam("inputCol"),
      input)

// chaining "stages" in an ML "PipelineStage" together
    for(s<-stages) {
      if(s != first) {
        s.set(
          s.getParam("inputCol"),
          input + n)
      }
      n += 1
      s.set(
        s.getParam("outputCol"),
        input + n)
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
