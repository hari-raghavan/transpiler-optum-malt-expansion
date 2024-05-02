package graph.Join_With_DB.config

import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Config(
  LOG_FILE_NM_JWD:     String = "",
  UNUSED_DATA_FILE_NM: String = ""
) extends ConfigBase

case class Context(spark: SparkSession, config: Config)
