package graph.TAL_Container_Assoc.config

import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession
import graph.TAL_Container_Assoc.Expand_TAL_Data_At_UDL_level.config.{
  Config => Expand_TAL_Data_At_UDL_level_Config
}

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Config(
  TAL_XWALK: String = "",
  Expand_TAL_Data_At_UDL_level: Expand_TAL_Data_At_UDL_level_Config =
    Expand_TAL_Data_At_UDL_level_Config()
) extends ConfigBase

case class Context(spark: SparkSession, config: Config)
