package ladydi.encoders

import org.apache.spark.SparkException
import org.apache.spark.ml.Transformer
import ladydi.forkedfromspark._
import org.apache.spark.ml.attribute.{UnresolvedAttribute, AttributeGroup, NumericAttribute, Attribute}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{Identifiable}
import org.apache.spark.mllib.linalg.{Vectors, Vector, VectorUDT}
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuilder

/**
  * Created by zafshar on 3/7/16.
  */
class ConcatStringEncoder (override val uid: String)
  extends Transformer with HasInputCols with HasOutputCol{

  def this() = this(Identifiable.randomUID("concatString"))

  /** @group setParam */
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: DataFrame): DataFrame = {
    // Schema transformation.
    val schema = dataset.schema
    lazy val first = dataset.first()
    val attrs = $(inputCols).flatMap { c =>
      val field = schema(c)
      val index = schema.fieldIndex(c)
      field.dataType match {
        case StringType =>
          val attr = Attribute.fromStructField(field)
          Some(attr.withName(c))
        case otherType =>
          throw new SparkException(s"ConcatStringEncoder does not support the $otherType type")
      }
    }
    val metadata = new AttributeGroup($(outputCol), attrs).toMetadata()

    // Data transformation.
    val assembleFunc = udf { r: Row =>
      ConcatStringEncoder.concat(r.toSeq: _*)
    }
    val args = $(inputCols).map { c =>
      schema(c).dataType match {
        case StringType => dataset(c)
      }
    }

    dataset.select(col("*"), assembleFunc(struct(args : _*)).as($(outputCol), metadata))
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputColNames = $(inputCols)
    val outputColName = $(outputCol)
    val inputDataTypes = inputColNames.map(name => schema(name).dataType)
    inputDataTypes.foreach {
      case _: StringType =>
      case other =>
        throw new IllegalArgumentException(s"Data type $other is not supported.")
    }
    if (schema.fieldNames.contains(outputColName)) {
      throw new IllegalArgumentException(s"Output column $outputColName already exists.")
    }
    StructType(schema.fields :+ new StructField(outputColName, StringType, true))
  }

  override def copy(extra: ParamMap): ConcatStringEncoder = defaultCopy(extra)

}

//note: cannot be saved to disk because Apache spark has
//decided to mark all relevant methods as "private"
object ConcatStringEncoder {

  def concat(vv: Any*): String = {

    vv.filter(_ != null).map {
      case v: String =>
        v
      case o =>
        throw new SparkException(s"$o of type ${o.getClass.getName} is not supported.")
    }.mkString(" ")
  }
}