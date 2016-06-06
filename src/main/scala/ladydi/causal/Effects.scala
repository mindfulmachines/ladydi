package ladydi.causal

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
  * Created by zafshar on 5/10/16.
  */

//TODO: Eventually add class called "Identify" which detects potential causal
//TODO: relations automatically.
//TODO: right NOW we only do resolution - look at below notes

class Effects {
  //note: first we need to detect how many categorical features we have
  //note: as of now, when we have no automated "causal detection" we will
  //require a  hard number so pass the number in your program and ladydi
  // will handle resolution


  def resolve(default: Map[String, Any], dataFrame: DataFrame): DataFrame = {
     default.foldLeft(dataFrame) { case (df, (k, v)) =>
      df.withColumn(k, lit(v))

    }
  }
}