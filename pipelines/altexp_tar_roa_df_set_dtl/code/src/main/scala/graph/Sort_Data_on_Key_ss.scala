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

object Sort_Data_on_Key_ss {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.orderBy(col("tar_roa_df_set_id").asc,
               col("tar_roa_df_set_dtl_id").asc,
               col("target_roa_cd").desc,
               col("target_dosage_form_cd").desc,
               col("priority").asc
    )

}
