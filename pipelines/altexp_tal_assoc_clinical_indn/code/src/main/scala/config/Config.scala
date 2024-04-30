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
  REJECT_FILE: String = "file:/altexp_tal_assoc_clinical_indn.20240429.rej",
  SORT_KEY:    String = "",
  OUTPUT_FILE: String = "file:/altexp_tal_assoc_clinical_indn.20240429.dat"
) extends ConfigBase
