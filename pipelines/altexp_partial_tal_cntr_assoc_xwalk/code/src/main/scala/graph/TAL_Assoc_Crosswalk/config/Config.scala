package graph.TAL_Assoc_Crosswalk.config

import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession
import graph.TAL_Assoc_Crosswalk.Expand_TAL_Assoc_Data_At_UDL_level.config.{
  Config => Expand_TAL_Assoc_Data_At_UDL_level_Config
}

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Config(
  UDL_CAG_EXP_FILE:      String = "",
  TAL_ASSOC_XWALK:       String = "",
  UDL_BASELINE_EXP_FILE: String = "",
  TAL_ASSOC_NAME:        String = "",
  CARRIER:               String = "",
  TAL_ASSOC_DTL:         String = "",
  CLIN_INDCN:            String = "",
  Expand_TAL_Assoc_Data_At_UDL_level: Expand_TAL_Assoc_Data_At_UDL_level_Config =
    Expand_TAL_Assoc_Data_At_UDL_level_Config()
) extends ConfigBase

case class Context(spark: SparkSession, config: Config)
