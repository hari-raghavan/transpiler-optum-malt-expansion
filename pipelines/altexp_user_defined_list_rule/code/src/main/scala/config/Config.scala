package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  DB_User:     String = "",
  DEDUP_KEY:   String = "",
  DB_Url:      String = "",
  DB_Password: String = "",
  DB_Driver:   String = "",
  REJECT_FILE: String = "file:/altexp_user_defined_list_rule.20240429.rej",
  SORT_KEY:    String = "{user_defined_list_id; user_defined_list_rule_id",
  OUTPUT_FILE: String = "file:/altexp_user_defined_list_rule.20240429.dat"
) extends ConfigBase
