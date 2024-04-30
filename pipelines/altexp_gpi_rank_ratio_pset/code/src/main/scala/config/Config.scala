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
  REJECT_FILE: String = "file:/altexp_gpi_rank_ratio.SXCDT-QA6.rej",
  SORT_KEY:    String = "",
  OUTPUT_FILE: String = "file:/altexp_gpi_rank_ratio.SXCDT-QA6.dat"
) extends ConfigBase
