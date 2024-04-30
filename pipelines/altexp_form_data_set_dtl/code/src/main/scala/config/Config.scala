package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  DB_User:       String = "",
  DEDUP_KEY:     String = "",
  DB_Url:        String = "",
  DB_Password:   String = "",
  BUSINESS_DATE: String = "20240429",
  DB_Driver:     String = "",
  REJECT_FILE:   String = "",
  SORT_KEY: String =
    "{formulary_name; carrier; account; group; customer_name; run_eff_dt; ndc11",
  OUTPUT_FILE: String = "file:/altexp_form_data_set_dtl.SXCDT-QA6.20240429.dat",
  ENV_NM:      String = "SXCDT-QA6"
) extends ConfigBase
