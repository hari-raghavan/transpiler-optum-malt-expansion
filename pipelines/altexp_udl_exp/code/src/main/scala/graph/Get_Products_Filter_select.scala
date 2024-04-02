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

object Get_Products_Filter_select {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.filter(
      when(
        (lit(context.config.CARRIER) === lit("NULL")).or(
          lookup("LKP_UDL_Master_CrossWalk", col("udl_id"))
            .getField("override_flg")
            .cast(BooleanType)
        ),
        lit(1).cast(BooleanType)
      ).otherwise(lit(0).cast(BooleanType)).cast(BooleanType)
    )

}
