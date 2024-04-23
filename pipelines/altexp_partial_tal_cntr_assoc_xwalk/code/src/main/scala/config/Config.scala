package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._
import graph.TAL_Container_Assoc.config.{Config => TAL_Container_Assoc_Config}
import graph.TAL_Assoc_Crosswalk.config.{Config => TAL_Assoc_Crosswalk_Config}
import graph.TAL_DTL_Crosswalk.config.{Config => TAL_DTL_Crosswalk_Config}

case class Config(
  TAL_ASSOC_NAME:    String = "",
  TAL_DTL_Crosswalk: TAL_DTL_Crosswalk_Config = TAL_DTL_Crosswalk_Config(),
  TAL_Container_Assoc: TAL_Container_Assoc_Config =
    TAL_Container_Assoc_Config(),
  TAL_Assoc_Crosswalk: TAL_Assoc_Crosswalk_Config = TAL_Assoc_Crosswalk_Config()
) extends ConfigBase
