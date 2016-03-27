package ladydi.encoders

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{DoubleType, DataType}

/**
  * Created by zafshar on 3/6/16.
  */


class NumericNullEncoder(override val uid: String)
  extends UnaryTransformer[java.lang.Number, Double, NumericNullEncoder]  {
Long
  def this() = this(Identifiable.randomUID("nullEncoder"))


  protected def createTransformFunc: java.lang.Number => Double = {
    NumericNullEncoder.transform
  }

   protected def outputDataType: DataType = DoubleType
}

object NumericNullEncoder {

  def load(path: String): NumericNullEncoder = new NumericNullEncoder()

  def transform (x: java.lang.Number): Double = {
    if (x == null) -1.0 ; else x.doubleValue()
  }
}
