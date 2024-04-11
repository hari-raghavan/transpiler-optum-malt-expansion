package graph.Association_Processing.config

import io.prophecy.libs._
import pureconfig._
import pureconfig.generic.ProductHint
import org.apache.spark.sql.SparkSession
import graph.Association_Processing.Alternative_Rollup_Processing.config.{
  Config => Alternative_Rollup_Processing_Config
}

object Config {

  implicit val confHint: ProductHint[Config] =
    ProductHint[Config](ConfigFieldMapping(CamelCase, CamelCase))

}

case class Config(
  PRODUCTS_FILE:            String = "",
  FORM_ALT_REJECT_FILE:     String = "",
  USER_ID:                  String = "",
  RUN_TS:                   String = "",
  ALIAS_PRODUCT_FILE:       Int = 0,
  REBATE_ELIGIBLE_FILE:     String = "",
  CLIN_INDCN:               String = "",
  constituent_group:        String = "",
  STEP_GRP_NM_XWALK:        String = "",
  FORMULARY_NM:             String = "",
  GPI_RANK_RATIO:           String = "",
  TAD_XWALK:                String = "",
  RULE_PRODUCTS_FILE:       String = "",
  STEP_GRP_NUM_XWALK:       String = "",
  FORM_CAG_OVRRD_DATA_PATH: String = "",
  FORM_TARGET_REJECT_FILE:  String = "",
  Alternative_Rollup_Processing: Alternative_Rollup_Processing_Config =
    Alternative_Rollup_Processing_Config()
) extends ConfigBase

case class Context(spark: SparkSession, config: Config)
