package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._
import graph.Association_Processing.config.{
  Config => Association_Processing_Config
}

case class Config(
  TARGET_LOAD_READY_FILE:    String = "",
  ALT_CLINC_LOAD_READY_FILE: String = "",
  CAG_TAL_CONTAINER_FILE:    String = "",
  ALT_PRXY_LOAD_READY_FILE:  String = "",
  ALT_LOAD_READY_FILE:       String = "",
  Association_Processing: Association_Processing_Config =
    Association_Processing_Config()
) extends ConfigBase
