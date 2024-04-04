package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  EXTRACT_FILE:       String = "",
  AI_SERIAL_HOME:     String = "",
  CAG_OVRRD_REF_FILE: String = "",
  OUTPUT_FILE_PREFIX: String = "altexp_",
  ENV_NM:             String = "RXCL3-CTR",
  BUSINESS_DATE:      String = "20240402"
) extends ConfigBase
