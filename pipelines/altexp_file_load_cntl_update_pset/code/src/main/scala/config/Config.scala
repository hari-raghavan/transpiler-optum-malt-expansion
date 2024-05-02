package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._
import graph.Join_With_DB.config.{Config => Join_With_DB_Config}

case class Config(
  JOIN_KEY:                   String = "",
  DB_User:                    String = "",
  SURR_KEY_INPUT_FILE_NM:     String = "",
  LOG_FILE_NM:                String = "",
  DB_Url:                     String = "",
  DB_Password:                String = "",
  db_databricks_secret_scope: String = "db",
  JOIN_DB_SQL:                String = "",
  INPUT_FILE_PATH:            String = "file:/",
  TABLE_NAME:                 String = "file_load_ctl_dtl",
  DB_Driver:                  String = "",
  SORT_KEY:                   String = "",
  LOAD_MODE:                  String = "U",
  Join_With_DB:               Join_With_DB_Config = Join_With_DB_Config(),
  FILE_NAME_W:                String = " ",
  DB_ALTERNATE_USER:          String = " ",
  LIST_OF_ALT_FILES:          List[String] = List(" ")
) extends ConfigBase
