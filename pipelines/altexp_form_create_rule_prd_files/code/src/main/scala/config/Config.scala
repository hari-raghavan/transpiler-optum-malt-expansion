package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  CAG_OVRRD_DATA_PATH:  String = "",
  RULE_PRODUCTS_FILE:   String = "",
  PRODUCTS_LOOKUP_FILE: String = "",
  FORM_RULE_PRODUCTS:   String = " "
) extends ConfigBase
