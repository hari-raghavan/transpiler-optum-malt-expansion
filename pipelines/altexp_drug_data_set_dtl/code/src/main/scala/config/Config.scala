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
    "{rxclaim_env_name; carrier; account; group; run_eff_dt; ndc11",
  OUTPUT_FILE: String = "file:/altexp_drug_data_set_dtl.RXCL2-CIG.dat",
  ENV_NM:      String = "RXCL2-CIG"
) extends ConfigBase
