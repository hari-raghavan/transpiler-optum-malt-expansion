package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  DB_User:               String = "",
  RUN_TS:                String = "2024-03-07 10:00:55",
  DB_ALTERNATE_USER:     String = "",
  DB_Url:                String = "",
  DB_Password:           String = "",
  DEFAULT_BUSINESS_DATE: String = "",
  DB_Driver:             String = ""
) extends ConfigBase
