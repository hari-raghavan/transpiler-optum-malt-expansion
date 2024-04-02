package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  FORM_RULE_PRODUCTS: String = "",
  TSD_EXTRACT:        String = "",
  FORM_TSD_XWALK:     String = "",
  TSD_NM:             String = "A5_CTR_STANDARD_3T",
  BUSINESS_DATE:      String = "20240401"
) extends ConfigBase
