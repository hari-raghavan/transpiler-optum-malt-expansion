package config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  TAR_DTL_LKP_FILE:         String = "",
  DB_User:                  String = "",
  FILE_LOAD_ERR_DTL_FILE:   String = "",
  RUN_TS:                   String = "",
  ALT_RUN_FILE:             String = "",
  DB_ALTERNATE_USER:        String = "",
  UDL_REF_FILE:             String = "",
  DB_Url:                   String = "",
  OUTPUT_PROFILE_FILE:      String = "",
  DB_Password:              String = "",
  TAC_DTL_LKP_FILE:         String = "",
  MASTER_REF_FILE:          String = "",
  FORM_REF_FILE:            String = "",
  TAL_DTL_LKP_FILE:         String = "",
  BUSINESS_DATE:            String = "",
  DB_Driver:                String = "",
  ALT_RUN_JOB_DETAILS_FILE: String = "",
  CAG_REF_FILE:             String = "",
  TSD_DTL_LKP_FILE:         String = ""
) extends ConfigBase
