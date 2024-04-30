package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  DB_User: String = "",
  DEDUP_KEY: String =
    "{alias_name; eff_dt; qual_priority; qual_id_value; search_txt",
  DB_Url:      String = "",
  DB_Password: String = "",
  LKP_FILE:    String = "file:/altexp_output_profile.SXCDT-QA6.dat",
  DB_Driver:   String = "",
  REJECT_FILE: String = "file:/altexp_alias_dtl.SXCDT-QA6.rej",
  SORT_KEY: String =
    "{alias_name; eff_dt; qual_priority; qual_id_value; search_txt; rank",
  OUTPUT_FILE: String = "file:/altexp_alias_dtl.SXCDT-QA6.dat"
) extends ConfigBase
