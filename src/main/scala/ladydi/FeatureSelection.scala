package ladydi

import java.util.concurrent.Executors

import org.apache.spark.sql.DataFrame

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Await, Future}

/**
  * Created by zafshar on 4/4/16.
  */
object FeatureSelection {

  case class FeatureType(val column: String, val datatype: String, val distinct: Long)

  def types (df : DataFrame) = {
    df.flatMap(r => (0 until r.size).map(i =>  (r.schema(i).name,
         if(r.get(i) == null) "" else r.get(i).toString
            ) ))
    .distinct()
    .countByKey()
    .map(r=> new FeatureType(r._1, df.schema(r._1).dataType.typeName, r._2))
    .toList
  }


  def categoricalFeatures(fields: List[FeatureType], excludedFeatures : List[String] = Nil) = {
    fields
      .filter(f => f.distinct <= 10 && f.distinct > 1)
      .map(_.column)
      .filter(! excludedFeatures.contains(_))

  }
  def numericFeatures (fields: List[FeatureType], excludedFeatures : List[String] = Nil) = {
    fields.
      filter(f => (f.datatype == "integer" || f.datatype =="double" ||
        f.datatype =="long") && f.distinct > 10)
      .map(_.column)
      .filter(! excludedFeatures.contains(_))


  }
  def textFeatures(fields: List[FeatureType], excludedFeatures : List[String] = Nil) = {
    fields
      .filter(f => f.datatype == "string" && f.distinct > 10)
      .map(_.column)
      .filter(! excludedFeatures.contains(_))

  }

  def allFeatures(df: DataFrame, excludedFeatures: List[String] = Nil) = {
    val featureTypes = types(df)

    numericFeatures(featureTypes, excludedFeatures) :::
      categoricalFeatures(featureTypes, excludedFeatures) :::
      textFeatures(featureTypes, excludedFeatures)
  }


  def select (n: Int = 1, df: DataFrame, excludedFeatures : List[String] = Nil,
              evaluator: List[String] => Double, p : Int = 1)
  : List[(Double, List[String])] = {

    implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(p))

    val features = allFeatures(df,excludedFeatures)

    var r : List[(Double, List[String])] = Nil
    var best : List[String] = Nil


    for (i <- 1 to n) {

      try {

        val results =
          features
            .filterNot(best contains _)
            .map(_ :: best)
            .map(
              f => {
                Future({
                  println(f);

                  try {
                    evaluator(f)
                  }

                  catch {
                    case w: Exception => println(s"MAJOR WARNING!!!!: The following features $f are MESSED UP!!" +
                      " I will contintue to run but you may want to check them out!")
                      10E8

                  }
                }, f)
              }
            ).map(Await.result(_, Duration.Inf)).sortBy(_._1)

        best = results.head._2
        r = results.head :: r
      } catch {
        case e: Exception => println("You seem to have broken feature selection!")
      }

    }
    r
  }
}
