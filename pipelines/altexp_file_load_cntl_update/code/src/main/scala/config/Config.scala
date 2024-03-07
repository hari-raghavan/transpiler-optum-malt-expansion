package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  DB_User:           String = "",
  hostname:          String = "",
  DB_Url:            String = "",
  DB_Password:       String = "",
  DB_Driver:         String = "",
  DB_ALTERNATE_USER: String = " "
) extends ConfigBase
