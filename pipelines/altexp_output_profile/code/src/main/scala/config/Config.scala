package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  DB_User:            String = "",
  FIRST_OF_NEXT_YEAR: String = "",
  DEDUP_KEY:          String = "",
  DB_Url:             String = "",
  DB_Password:        String = "",
  BUSINESS_DATE:      String = "20240429",
  DB_Driver:          String = "",
  REJECT_FILE:        String = "file:/altexp_output_profile.SXCDT-QA6.rej",
  SORT_KEY: String =
    "{carrier; account; group; future_flg; as_of_dt; output_profile_id; alias_priority",
  OUTPUT_FILE: String = "file:/altexp_output_profile.SXCDT-QA6.dat",
  ENV_NM:      String = "SXCDT-QA6"
) extends ConfigBase
