package ladydi.encoders

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{DoubleType, DataType}

/**
  * Created by zafshar on 3/6/16.
  */


class NullEncoder (override val uid: String)
  extends UnaryTransformer[java.lang.Double, Double, NullEncoder]  {

  def this() = this(Identifiable.randomUID("nullEncoder"))


  protected def createTransformFunc: java.lang.Double => Double = {
    NullEncoder.transform
  }

   protected def outputDataType: DataType = DoubleType
}

object NullEncoder {

  def load(path: String): NullEncoder = new NullEncoder()

  def transform (x: java.lang.Double): Double = {
    if (x == null) -1.0 ; else x
  }
}
