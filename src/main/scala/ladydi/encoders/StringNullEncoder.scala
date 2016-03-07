package ladydi.encoders

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{StringType, DoubleType, DataType}

/**
  * Created by zafshar on 3/7/16.
  */
class StringNullEncoder (override val uid: String)
  extends UnaryTransformer[java.lang.String, String, StringNullEncoder]  {

  def this() = this(Identifiable.randomUID("nullEncoder"))


  protected def createTransformFunc: java.lang.String => String = {
    StringNullEncoder.transform
  }

  protected def outputDataType: DataType = StringType

}

object StringNullEncoder {

  def load(path: String): StringNullEncoder = new StringNullEncoder()

  def transform (x: java.lang.String): String = {
    if (x == null) "" ; else x
  }
}