package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  DB_User:     String = "",
  DEDUP_KEY:   String = "",
  DB_Url:      String = "",
  DB_Password: String = "",
  LKP_FILE:    String = "file:/altexp_tar_dtl.20240429.dat",
  DB_Driver:   String = "",
  REJECT_FILE: String = "file:/altexp_tar_roa_df_set_dtl.20240429.rej",
  SORT_KEY: String =
    "{tar_roa_df_set_id; tar_roa_df_set_dtl_id; target_roa_cd descending; target_dosage_form_cd descending; priority",
  OUTPUT_FILE: String = "file:/altexp_tar_roa_df_set_dtl.20240429.dat"
) extends ConfigBase
