package graph

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Filter_Exclusion_Association {

  def apply(context: Context, in: DataFrame): (DataFrame, DataFrame) =
    (in.filter(col("tal_assoc_type_cd").cast(StringType) === lit(3)),
     in.filter(col("tal_assoc_type_cd").cast(StringType) =!= lit(3))
    )

}
