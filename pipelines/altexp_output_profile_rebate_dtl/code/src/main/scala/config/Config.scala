package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  DB_User:     String = "",
  DEDUP_KEY:   String = "",
  DB_Url:      String = "",
  DB_Password: String = "",
  LKP_FILE:    String = "file:/altexp_output_profile.RXBK1-PRD.dat",
  DB_Driver:   String = "",
  REJECT_FILE: String = "",
  SORT_KEY:    String = "{output_profile_id;rebate_elig_cd",
  OUTPUT_FILE: String = "file:/altexp_output_profile_rebate_dtl.RXBK1-PRD.dat"
) extends ConfigBase
