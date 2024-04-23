package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  IN_FILE:       String = "",
  SFTP_HOST:     String = "",
  SFTP_USER:     String = "",
  SFTP_PASSWORD: String = ""
) extends ConfigBase
