package graph.TAL_DTL_Crosswalk.config

import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Config(
  TAL_NAME:  String = "",
  TAL_DTL:   String = "",
  TAL_XWALK: String = ""
) extends ConfigBase

case class Context(spark: SparkSession, config: Config)
