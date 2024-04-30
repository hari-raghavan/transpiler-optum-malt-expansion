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
  REJECT_FILE: String = "file:/altexp_tad.RXCL2-CIG.rej",
  SORT_KEY:    String = "{target_gpi14;rank",
  OUTPUT_FILE: String = "file:/altexp_tad.RXCL2-CIG.dat"
) extends ConfigBase
