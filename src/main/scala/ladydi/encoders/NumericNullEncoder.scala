package ladydi.encoders

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{DoubleType, DataType}
import org.apache.spark.mllib.linalg._

/**
  * Created by zafshar on 3/6/16.
  */


class NumericNullEncoder(override val uid: String)
  extends UnaryTransformer[java.lang.Number, Vector, NumericNullEncoder]  {
Long
  def this() = this(Identifiable.randomUID("nullEncoder"))


  protected def createTransformFunc: java.lang.Number => Vector = {
    NumericNullEncoder.transform
  }

   protected def outputDataType: DataType = new VectorUDT
}

object NumericNullEncoder {

  def load(path: String): NumericNullEncoder = new NumericNullEncoder()

  def transform (x: java.lang.Number): Vector = {
    if (x == null || x.doubleValue().isNaN || x.doubleValue().isInfinity) {
      new DenseVector(Array(1.0,-1.0, -1.0)) ;
    }
    else {
      new DenseVector(Array(0.0, x.doubleValue(), math.log(x.doubleValue())))
    }
  }
}
