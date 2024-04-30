package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._
import graph.Join_With_DB.config.{Config => Join_With_DB_Config}

case class Config(
  JOIN_KEY: String = "{formulary_name; tal_assoc_name; target_ndc}",
  DB_User:  String = "",
  SURR_KEY_INPUT_FILE_NM: String =
    "/altexp_alt_run_target_dtl_surr_key.SXCDT-QA6..dat",
  LOG_FILE_NM:       String = "",
  DB_Url:            String = "",
  DB_Password:       String = "",
  JOIN_DB_SQL:       String = "",
  INPUT_FILE_PATH:   String = "",
  TABLE_NAME:        String = "alt_run_clinical_indn_dtl",
  DB_Driver:         String = "",
  SORT_KEY:          String = "{formulary_name; tal_assoc_name; target_ndc",
  LOAD_MODE:         String = "A",
  Join_With_DB:      Join_With_DB_Config = Join_With_DB_Config(),
  LIST_OF_ALT_FILES: List[String] = List(" ")
) extends ConfigBase
