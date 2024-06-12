package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  EXTRACT_FILE:       String = "",
  AI_SERIAL_HOME:     String = "",
  FIRST_OF_NEXT_YEAR: String = "20250101",
  CAG_OVRRD_REF_FILE: String =
    "AI_SERIAL_HOME/deliver/./altexp_form_ovrrd_ref.dat",
  OUTPUT_FILE_PREFIX: String = "altexp_ ",
  ENV_NM:             String = "RXCL3-CTR",
  BUSINESS_DATE:      String = "20240401"
) extends ConfigBase
