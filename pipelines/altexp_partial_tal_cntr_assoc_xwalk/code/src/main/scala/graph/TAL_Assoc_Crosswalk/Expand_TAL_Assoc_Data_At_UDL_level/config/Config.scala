package graph.TAL_Assoc_Crosswalk.Expand_TAL_Assoc_Data_At_UDL_level.config

import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Config(TAL_ASSOC_XWALK: String = "") extends ConfigBase
case class Context(spark: SparkSession, config: Config)
