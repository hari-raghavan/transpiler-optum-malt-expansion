package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  RULE_PRODUCTS_FILE:  String = "",
  CAG_OVRRD_DATA_PATH: String = "",
  BUSINESS_DATE:       String = "20240401",
  PRODUCTS_FILE:       String = "",
  PRODUCT_FILE:        String = " "
) extends ConfigBase
