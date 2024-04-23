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

object Scan_Remove_Exclusion_Products_from_Inclusion_and_Standard_Association_output_select {

  def apply(context: Context, in: DataFrame): DataFrame =
    in.filter(
      when(col("tal_assoc_type_cd").cast(StringType) === lit(3), lit(0))
        .when(
          !size(col("target_prdcts")).cast(BooleanType),
          force_error(
            concat(
              lit("TAL Assocation : "),
              col("tal_assoc_name"),
              lit(" with type code "),
              col("tal_assoc_type_cd").cast(StringType),
              lit(
                " do not have Target Products after applying Exlcusion Association"
              )
            ).cast(StringType)
          )
        )
        .otherwise(lit(1))
        .cast(BooleanType)
    )

}
