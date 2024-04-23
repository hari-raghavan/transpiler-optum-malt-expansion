package graph.TAL_Container_Assoc.Expand_TAL_Data_At_UDL_level.config

import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Config(CAG_TAL_CONTAINER_FILE: String = "") extends ConfigBase
case class Context(spark: SparkSession, config: Config)
