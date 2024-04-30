package graph

import io.prophecy.libs._
import config.Context
import udfs.UDFs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Read_load_ready_files {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
      val windowSpec        = Window.partitionBy(lit(1)).rowsBetween(Window.unboundedPreceding, Window.currentRow)
      val windowSpecPrevRow = Window.partitionBy(lit(1)).rowsBetween(Window.unboundedPreceding, -1)
      val windowSpecL       = Window.partitionBy(lit(1)).orderBy(lit(1))
    
      lazy val dfWithUniqueId = in.zipWithIndex(0, 1, "tempId", spark)
    
      val fileDF = dfWithUniqueId.select(col("tempId").as("tempId"),
                                         concat(lit(Config.INPUT_FILE_PATH), lit("/"), col("line")).as("fileName")
      )
    
      val fileRecordDF = dfWithUniqueId.mergeMultipleFileContentInDataFrame(
        fileDF,
        spark,
        delimiter = ",",
        abinitioSchema =
          """type qual_priority_t =
    record
      string(int) qual;
      decimal("") priority;
    end;
    constant string(int)[int] OVERRIDE_QUALIFIERS = [vector 'DESI_CD', 'DOSAGE_FORM', 'MSC', 'ROA', 'RXOTC' ];
    constant qual_priority_t[int] QUAL_PRIORITY =[vector [record  qual "NDC11"        priority "1"],
                                                          [record qual "NDC9"         priority "2"],
                                                          [record qual "NDC5"         priority "3"],
                                                          [record qual "GPI14"        priority "4"],
                                                          [record qual "GPI12"        priority "5"],
                                                          [record qual "GPI10"        priority "6"],
                                                          [record qual "DOSAGE_FORM"  priority "7"],
                                                          [record qual "ROA"          priority "8"],
                                                          [record qual "DRUG_NAME"    priority "9"],
                                                          [record qual "GPI8"         priority "10"],
                                                          [record qual "GPI6"         priority "11"],
                                                          [record qual "GPI4"         priority "12"],
                                                          [record qual "MSC"          priority "13"],
                                                          [record qual "RXOTC"        priority "14"],
                                                          [record qual "DAYS_UNTIL_DRUG_STATUS_INACTIVE" priority "15"] ,
                                                          [record qual "STATUS_CD"  priority   "16"],
                                                          [record qual "REPACKAGER"   priority "17"],
                                                          [record qual "DESI_CD"    priority   "18"]];
    type drug_data_set_dtl_t =
    record
      string("\x01", maximum_length=20) rxclaim_env_name ;
      string("\x01", maximum_length=20) carrier = NULL("") ;
      string("\x01", maximum_length=20) account = NULL("") ;
      string("\x01", maximum_length=20) group = NULL("") ;
      date("YYYYMMDD")("\x01") run_eff_dt = NULL("") ;
      decimal("\x01",0, maximum_length=10) drug_data_set_dtl_id ;
      decimal("\x01",0, maximum_length=10) drug_data_set_id ;
      string("\x01", maximum_length=11) ndc11 ;
      string("\x01", maximum_length=14) gpi14 ;
      string("\x01", maximum_length=1) status_cd ;
      string("\x01", maximum_length=8) eff_dt = NULL("") ;
      string("\x01", maximum_length=8) term_dt = NULL("") ;
      string("\x01", maximum_length=8) inactive_dt = NULL("") ;
      string("\x01", maximum_length=1) msc ;
      string("\x01", maximum_length=70) drug_name ;
      string("\x01", maximum_length=30) prod_short_desc ;   
      string("\x01", maximum_length=3) rx_otc ;
      string("\x01", maximum_length=1) rx_otc_cd ;
      string("\x01", maximum_length=1) desi ;
      string("\x01", maximum_length=2) roa_cd ;
      string("\x01", maximum_length=4) dosage_form_cd ;
      decimal("\x01".5, maximum_length=15) prod_strength ;
      string("\x01", maximum_length=1) repack_cd ;   
      string("\x01", maximum_length=60) gpi14_desc ;
      string("\x01", maximum_length=60) gpi8_desc ;
      string(1) newline = "\n";
    end;
    type user_defined_list_rule_t =
    record
      decimal("\x01",0, maximum_length=10) user_defined_list_id ;
      string("\x01", maximum_length=20) user_defined_list_name ;
      string("\x01", maximum_length=60) user_defined_list_desc ;
      decimal("\x01",0, maximum_length=10) user_defined_list_rule_id ;
      string("\x01", maximum_length=4000) rule = NULL("") ;
      string("\x01", maximum_length=1) incl_cd ;
      date("YYYYMMDD")("\x01") eff_dt ;
      date("YYYYMMDD")("\x01") term_dt ;
      string(1) newline = "\n";
    end;
    type rule_xwalk_t=
    record
      decimal("\x01") udl_id;
      decimal("\x01") udl_rule_id;
      string("\x01") udl_nm;
      string("\x01") udl_desc;
      decimal("\x01") rule_priority;
      string(1) inclusion_cd;
      string(int) qualifier_cd;
      string(int) operator;
      string(int) compare_value;
      string(1) conjunction_cd;
      decimal("\x01") rule_expression_id;
      date("YYYYMMDD")("\x01") eff_dt ;
      date("YYYYMMDD")("\x01") term_dt ;
      string(1) newline = "\n";
    end;
    type udl_mstr_xwalk_t=
    record
      decimal("\x01",0, maximum_length=10) udl_id;
      string("\x01", maximum_length=20) udl_nm;
      string("\x01", maximum_length=60) user_defined_list_desc;
      string(int)[int] qual_list;
      decimal(1) override_flg;
      date("YYYYMMDD")("\x01") eff_dt ;
      date("YYYYMMDD")("\x01") term_dt ;
      string(1) newline = "\n";
    end;
    type product_lkp_t =
    record
      int dl_bit;
      string("\x01", maximum_length=11)  ndc11 ;
      string("\x01", maximum_length=14)  gpi14 ;
      string("\x01", maximum_length=1)  status_cd ;
      string("\x01", maximum_length=8) inactive_dt ;
      string("\x01", maximum_length=1)  msc ;
      string("\x01", maximum_length=70)  drug_name ;
      string("\x01", maximum_length=3)  rx_otc ;
      string("\x01", maximum_length=1)  desi ;
      string("\x01", maximum_length=2)  roa_cd ;
      string("\x01", maximum_length=4)  dosage_form_cd ;
      decimal("\x01".5, maximum_length=15) prod_strength ;
      string("\x01", maximum_length=1)  repack_cd ; 
      string("\x01", maximum_length=30) prod_short_desc ;
      string("\x01", maximum_length=60) gpi14_desc ;
      string("\x01", maximum_length=60) gpi8_desc ;
      string(1) newline = "\n";
    end;
    type nrmlz_dataset_qual_t = 
    record
      int dl_bit;
      string(int) qualifier_cd;
      string(int) compare_value = NULL;
    end;
    type rule_product_lkp_t =
    record
      string(int) qualifier_cd;
      string(int) operator;
      string(int) compare_value;
      bit_vector_t products;
    end;
    type cag_ovrrd_xwalk_t =
    record
      string("\x01", maximum_length=20)  carrier = NULL("") ;
      string("\x01", maximum_length=20)  account = NULL("") ;
      string("\x01", maximum_length=20)  group = NULL("") ;
      string("\x01", maximum_length=11)  ndc11 ;
      string("\x01", maximum_length=14)  gpi14 ;
      string("\x01", maximum_length=1)  status_cd ;
      string("\x01", maximum_length=8) eff_dt = NULL("") ;
      string("\x01", maximum_length=8) term_dt = NULL("") ;
      string("\x01", maximum_length=8) inactive_dt ;
      string("\x01", maximum_length=1)  msc ;
      string("\x01", maximum_length=70)  drug_name ;
      string("\x01", maximum_length=3)  rx_otc ;
      string("\x01", maximum_length=1)  desi ;
      string("\x01", maximum_length=2)  roa_cd ;
      string("\x01", maximum_length=4)  dosage_form_cd ;
      decimal("\x01".5, maximum_length=15) prod_strength ;
      string("\x01", maximum_length=1)  repack_cd ;
      string("\x01", maximum_length=30) prod_short_desc ;
      string("\x01", maximum_length=60) gpi14_desc ;
      string("\x01", maximum_length=60) gpi8_desc ;
      string(1) newline = "\n";
    end;
    type cag_ovrrd_ref_file_t =
    record
      string("\x01", maximum_length=20)  carrier = NULL("") ;
      string("\x01", maximum_length=20)  account = NULL("") ;
      string("\x01", maximum_length=20)  group = NULL("") ;
      decimal("\x01",0, maximum_length=1) is_future_snap = 0;
      string("\x01") data_path;
      string(1) newline = "\n";
    end;
    type udl_exp_t =
    record
      decimal("\x01",0, maximum_length=10) udl_id;
      string("\x01", maximum_length=20) udl_nm; 
      string("\x01", maximum_length=60) udl_desc; 
      bit_vector_t products;
      date("YYYYMMDD")("\x01") eff_dt ;
      date("YYYYMMDD")("\x01") term_dt ;
      bit_vector_t[int] contents;
      string(1) newline = "\n";
    end;
    type tal_dtl_t = 
    record
      decimal("\x01",0, maximum_length=10) tal_dtl_id ;
      decimal("\x01",0, maximum_length=10) tal_id ;
      string("\x01", maximum_length=20) tal_name ;
      string("\x01", maximum_length=60) tal_desc ;
      decimal("\x01",0, maximum_length=39) tal_dtl_type_cd ;
      string("\x01", maximum_length=20) nested_tal_name = NULL("") ;
      string("\x01", maximum_length=20) tal_assoc_name = NULL("") ;
      decimal("\x01", 6, maximum_length=39) priority ;
      date("YYYYMMDD")("\x01") eff_dt ;
      date("YYYYMMDD")("\x01") term_dt ;
      string(1) newline = "\n";
    end;
    type tal_assoc_dtl_t = 
    record
      decimal("\x01",0, maximum_length=10) tal_assoc_dtl_id ;
      decimal("\x01",0, maximum_length=10) tal_assoc_id ;
      string("\x01", maximum_length=20) tal_assoc_name ;
      string("\x01", maximum_length=60) tal_assoc_desc ;
      decimal("\x01",0, maximum_length=39) tal_assoc_type_cd ;
      string("\x01", maximum_length=20) target_udl_name = NULL("") ;
      string("\x01", maximum_length=20) alt_udl_name = NULL("") ;
      decimal("\x01",0, maximum_length=39) alt_rank = NULL("") ;
      decimal("\x01",0, maximum_length=39) constituent_rank = NULL("") ;
      string("\x01", maximum_length=2) constituent_group = NULL("") ;
      string("\x01", maximum_length=1) constituent_reqd = NULL("") ;
      string("\x01", maximum_length=30) shared_qual ;
      string("\x01", maximum_length=20) override_tac_name = NULL("") ;
      string("\x01", maximum_length=20) override_tar_name = NULL("") ;
      string(1) newline = "\n";
    end;
    type tac_dtl_t = 
    record
      decimal("\x01",0, maximum_length=10) tac_dtl_id ;
      decimal("\x01",0, maximum_length=10) tac_id ;
      string("\x01", maximum_length=20) tac_name ;
      decimal("\x01",0, maximum_length=39) priority ;
      string("\x01", maximum_length=4000) target_rule = NULL("") ;
      string("\x01", maximum_length=4000) alt_rule = NULL("") ;
      date("YYYYMMDD")("\x01") eff_dt ;
      date("YYYYMMDD")("\x01") term_dt ;
      string(1) newline = "\n";
    end;
    type tac_rule_xwalk_t =
    record
      decimal("\x01",0, maximum_length=10) tac_id ;
      string("\x01", maximum_length=20) tac_name ;
      decimal("\x01",0, maximum_length=39) priority ;
      date("YYYYMMDD")("\x01") eff_dt ;
      date("YYYYMMDD")("\x01") term_dt ;
      record
       string(int) qualifier_cd;
       string(int) operator;
       string(int) compare_value;
       string(1) conjunction_cd;
      end[int] target_rule_def;
      record
       string(int) qualifier_cd;
       string(int) operator;
       string(int) compare_value;
       string(1) conjunction_cd;
      end[int] alt_rule_def;
      string(1) newline = "\n";
    end;
    type contents_alt_st =
    record
     bit_vector_t target_prdcts;
     bit_vector_t alternate_prdcts;
     string("\x01", maximum_length=40) tal_assoc_name;
    end;
    type tac_st_contents =
    record
      decimal("\x01",0, maximum_length=10) tal_id;
      contents_alt_st[int] step_contents;
      string(1) newline = '\n';
    end;
    type contents_t =
    record
      bit_vector_t target_prdcts;
      bit_vector_t alt_prdcts;
      decimal(1) ST_flag =0;
      decimal(1) priority;
    end;
    type tac_contents_t =
    record
     bit_vector_t target_prdcts;
     bit_vector_t alt_prdcts;
     contents_t[int] contents;
     decimal(1) xtra_proc_flg = 0;
    end;
    type tac_xwalk_t =
    record
      decimal("\x01",0, maximum_length=10) tac_id ;
      string("\x01", maximum_length=20) tac_name ;
      tac_contents_t[int] tac_contents;
      date("YYYYMMDD")("\x01") eff_dt ;
      date("YYYYMMDD")("\x01") term_dt ;
      string(1) newline = '\n';
    end;
    type tal_container_xwalk_t =
    record
      string("\x01", maximum_length=20) tal_name ;
      int target_dl_bit;
      record
       int alt_prd;
       int alt_rank;
      end[int] alt_prdcts;
      string(1) newline = "\n";
    end;
    type target_udl_products_t =
    record
      string("\x01", maximum_length=20) target_udl_name = NULL("") ;
      bit_vector_t target_products;
    end;
    type alt_udl_products_t =
    record
      string("\x01", maximum_length=20) alt_udl_name = NULL("") ;
      decimal("\x01",0, maximum_length=39) alt_rank = NULL("") ;
      bit_vector_t alt_products;
    end;
    type alt_exclusion_products_t =
    record
      decimal("\x01",0, maximum_length=10) tal_assoc_dtl_id ;
      decimal("\x01",0, maximum_length=10) tal_assoc_id ;
      string("\x01", maximum_length=20) tal_assoc_name ;
      decimal("\x01",0, maximum_length=39) tal_assoc_type_cd ;
      string("\x01", maximum_length=20) override_tac_name = NULL("") ;
      string("\x01", maximum_length=20) override_tar_name = NULL("") ;
      bit_vector_t target_product_dtl;
      bit_vector_t alt_product_dtl;
      string(1) newline = "\n";
    end;
    type tal_assoc_xwalk_t =
    record
      decimal("\x01",0, maximum_length=10) tal_assoc_id ;
      string("\x01", maximum_length=20) tal_assoc_name ;
      string("\x01", maximum_length=30) shared_qual ;
      string("\x01", maximum_length=10) override_tac_name = NULL("") ;
      string("\x01", maximum_length=10) override_tar_name = NULL("") ;
      decimal("\x01",0, maximum_length=39) tal_assoc_type_cd ;
      target_udl_products_t[int] target_udl_products;
      alt_udl_products_t[int] alt_udl_products;
      string(1) newline = "\n";
    end;
    type form_data_set_dtl_t =
    record
      decimal("\x01",0, maximum_length=10) formulary_data_set_id ;
      string("\x01", maximum_length=20) formulary_name ;
      string("\x01", maximum_length=20) formulary_id = NULL("") ;
      string("\x01", maximum_length=10) formulary_cd = "" ;
      string("\x01", maximum_length=20) carrier = NULL("") ;
      string("\x01", maximum_length=20) account = NULL("") ;
      string("\x01", maximum_length=20) group = NULL("") ;
      string("\x01", maximum_length=20) rxclaim_env_name ;
      string("\x01", maximum_length=20) customer_name = NULL("");
      date("YYYYMMDD")("\x01") last_exp_dt ;
      date("YYYYMMDD")("\x01") run_eff_dt = NULL("") ;
      decimal("\x01",0, maximum_length=10) formulary_data_set_dtl_id ;
      string("\x01", maximum_length=11) ndc11 ;
      string("\x01", maximum_length=2) formulary_tier ;
      string("\x01", maximum_length=2) formulary_status ;
      string("\x01", maximum_length=1) pa_reqd_ind ;
      string("\x01", maximum_length=1) specialty_ind ;
      string("\x01", maximum_length=1) step_therapy_ind ;
      string("\x01", maximum_length=50) formulary_tier_desc ;
      string("\x01", maximum_length=50) formulary_status_desc ;
      string("\x01", maximum_length=1) pa_type_cd ;
      string("\x01", maximum_length=1) step_therapy_type_cd ;
      string("\x01", maximum_length=100) step_therapy_group_name = NULL("") ;
      decimal("\x01",0, maximum_length=39) step_therapy_step_number = NULL("") ;
      string(1) newline = "\n";
    end;
    type form_ovrrd_xwalk_t =
    record
      string("\x01", maximum_length=20) formulary_name ;
      string("\x01", maximum_length=10) formulary_cd = "" ;
      string("\x01", maximum_length=20)  carrier = NULL("") ;
      string("\x01", maximum_length=20)  account = NULL("") ;
      string("\x01", maximum_length=20)  group = NULL("") ;
      date("YYYYMMDD")("\x01") last_exp_dt ;
      string("\x01", maximum_length=11) ndc11 ;
      string("\x01", maximum_length=2) formulary_tier ;
      string("\x01", maximum_length=2) formulary_status ;
      string("\x01", maximum_length=1) pa_reqd_ind ;
      string("\x01", maximum_length=1) specialty_ind ;
      string("\x01", maximum_length=1) step_therapy_ind ;
      string("\x01", maximum_length=50) formulary_tier_desc ;
      string("\x01", maximum_length=50) formulary_status_desc ;
      string("\x01", maximum_length=1) pa_type_cd ;
      string("\x01", maximum_length=1) step_therapy_type_cd ;
      string("\x01", maximum_length=100) step_therapy_group_name = NULL("") ;
      decimal("\x01",0, maximum_length=39) step_therapy_step_number = NULL("") ;
      string(1) newline = "\n";
    end;
    type form_ovrrd_ref_file_t =
    record
      string("\x01", maximum_length=20) formulary_name ;
      string("\x01", maximum_length=20)  carrier = NULL("") ;
      string("\x01", maximum_length=20)  account = NULL("") ;
      string("\x01", maximum_length=20)  group = NULL("") ;
      string("\x01", maximum_length=20) customer_name = NULL("");
      decimal("\x01",0, maximum_length=1) is_future_snap = 0;
      string("\x01") data_path;
      date("YYYYMMDD")("\x01") run_eff_dt = NULL("") ;
      string(1) newline = "\n";
    end;
    type output_profile_t =
    record
      decimal("\x01",0, maximum_length=10) output_profile_id ;
      string("\x01", maximum_length=20) rxclaim_env_name ;
      string("\x01", maximum_length=20) formulary_name ;
      string("\x01", maximum_length=20) formulary_id = NULL("") ;
      decimal("\x01",0, maximum_length=10) output_profile_form_dtl_id ;
      decimal("\x01",0, maximum_length=10) output_profile_job_dtl_id ;
      string("\x01", maximum_length=20) output_profile_name ;
      string("\x01", maximum_length=20) alias_name = NULL("") ;
      decimal("\x01",0, maximum_length=39) alias_priority = NULL ;
      string("\x01", maximum_length=20) carrier = NULL("") ;
      string("\x01", maximum_length=20) account = NULL("") ;
      string("\x01", maximum_length=20) group = NULL("") ;
      string("\x01", maximum_length=20) tal_name ;
      string("\x01", maximum_length=20) tac_name ;
      string("\x01", maximum_length=20) tar_name ;
      string("\x01", maximum_length=20) tsd_name ;
      decimal("\x01",0, maximum_length=10) job_id ;
      string("\x01", maximum_length=20) job_name ;
      string("\x01", maximum_length=20) customer_name = NULL("");
      decimal("\x01",0, maximum_length=39) run_day = NULL("") ;
      string("\x01", maximum_length=20) lob_name = NULL("") ;
      string("\x01", maximum_length=4) run_jan1_start_mmdd = NULL("") ;
      string("\x01", maximum_length=4) run_jan1_end_mmdd = NULL("") ;
      decimal("\x01") future_flg = 0;
      string("\x01", maximum_length=30) formulary_pseudonym = NULL("") ;
      decimal("\x01",0, maximum_length=10) notes_id = NULL("") ;
      string("\x01", maximum_length=60) output_profile_desc ;
      decimal("\x01",0, maximum_length=39 ) formulary_option_cd ;
      string("\x01", maximum_length=20) layout_name ;  
      date("YYYYMMDD")("\x01") as_of_dt = NULL("") ;
      string("\x01", maximum_length=1) st_tac_ind ;
      string(1) newline = "\n";
    end;
    type form_product_t =
    record
      string("\x01", maximum_length=10) formulary_cd = "" ;
      string("\x01", maximum_length=11) ndc11 ;
      string("\x01", maximum_length=2) formulary_tier ;
      string("\x01", maximum_length=2) formulary_status ;
      string("\x01", maximum_length=1) pa_reqd_ind ;
      string("\x01", maximum_length=1) specialty_ind ;
      string("\x01", maximum_length=1) step_therapy_ind ;
      string("\x01", maximum_length=50) formulary_tier_desc ;
      string("\x01", maximum_length=50) formulary_status_desc ;
      string("\x01", maximum_length=1) pa_type_cd ;
      string("\x01", maximum_length=1) step_therapy_type_cd ;
      string("\x01", maximum_length=100) step_therapy_group_name = NULL("") ;
      decimal("\x01",0, maximum_length=39) step_therapy_step_number = NULL("") ;
      date("YYYYMMDD")("\x01") last_exp_dt ;
    end;
    type cag_product_t =
    record
      string("\x01", maximum_length=11) ndc11 ;
      string("\x01", maximum_length=14) gpi14 ;
      string("\x01", maximum_length=1) status_cd ;
      string("\x01", maximum_length=8) eff_dt = NULL("") ;
      string("\x01", maximum_length=8) term_dt = NULL("") ;
      string("\x01", maximum_length=8) inactive_dt ;
      string("\x01", maximum_length=1) msc ;
      string("\x01", maximum_length=70) drug_name ;
      string("\x01", maximum_length=3) rx_otc ;
      string("\x01", maximum_length=1) desi ;
      string("\x01", maximum_length=2) roa_cd ;
      string("\x01", maximum_length=4) dosage_form_cd ;
      decimal("\x01".5, maximum_length=15) prod_strength ;
      string("\x01", maximum_length=1) repack_cd ;
      string("\x01", maximum_length=30) prod_short_desc ;
      string("\x01", maximum_length=60) gpi14_desc ;
      string("\x01", maximum_length=60) gpi8_desc ;
    end;
    type cag_rlp_t =
    record
      string("\x01", maximum_length=20) carrier = NULL("") ;
      string("\x01", maximum_length=20) account = NULL("") ;
      string("\x01", maximum_length=20) group = NULL("") ;
      date("YYYYMMDD")("\x01") run_eff_dt = NULL("") ;
      decimal(1) cag_priority = 0;
      cag_product_t[int] prdcts;
    end;
    type form_rlp_t =
    record
      string("\x01", maximum_length=20) formulary_name ;
      string("\x01", maximum_length=20) carrier = NULL("") ;
      string("\x01", maximum_length=20) account = NULL("") ;
      string("\x01", maximum_length=20) group = NULL("") ;
      string("\x01", maximum_length=20) customer_name = NULL("");
      date("YYYYMMDD")("\x01") run_eff_dt = NULL("") ;
      decimal(1) cag_priority = 0;
      form_product_t[int] prdcts; 
    end;
    type tsd_dtl_t =
    record
      decimal("\x01",0, maximum_length=10) tsd_dtl_id ;
      decimal("\x01",0, maximum_length=10) tsd_id ;
      string("\x01", maximum_length=20) tsd_name ;
      string("\x01", maximum_length=30) tsd_cd ;
      string("\x01", maximum_length=2) formulary_tier ;
      string("\x01", maximum_length=2) formulary_status ;
      decimal("\x01",0, maximum_length=39) priority ;
      date("YYYYMMDD")("\x01") eff_dt ;
      date("YYYYMMDD")("\x01") term_dt ;
      string(1) newline = "\n";
    end;
    type tsd_xwalk_t =
    record
      decimal("\x01",0, maximum_length=10) tsd_id ;
      string("\x01", maximum_length=30) tsd_cd ;
      bit_vector_t products;
      string(1) newline = "\n";
    end;
    type alt_constituent_prdct_t = 
    record
      bit_vector_t alt_prdcts;
      string(2) constituent_group = NULL("");
      string(1) constituent_reqd = NULL(""); 
      string(20) udl_nm = NULL(""); 
    end;
    type tal_container_t =
    record
      decimal("\x01",0, maximum_length=10) tal_id ;
      string("\x01", maximum_length=20) tal_name ;
      string("\x01", maximum_length=20) tal_assoc_name = NULL("") ;
      string("\x01", maximum_length=20) tar_udl_nm = NULL("") ;
      string("\x01", maximum_length=60) tal_desc ;
      decimal("\x01", 6, maximum_length=39) priority ;
      decimal("\x01",0, maximum_length=39) tal_assoc_type_cd ;
      bit_vector_t[int] target_prdcts;
      alt_constituent_prdct_t[int] alt_constituent_prdcts;
      string("\x01", maximum_length=30) shared_qual ;
      string("\x01", maximum_length=20) override_tac_name = NULL("") ;
      string("\x01", maximum_length=20) override_tar_name = NULL("") ;
      string(2)[int] constituent_grp_vec;
      string(1) newline = "\n";
    end;
    type master_cag_mapping_t =
    record
      string("\x01") carrier = NULL("") ;
      string("\x01") account = NULL("") ;
      string("\x01") group = NULL("") ;
      string("\x01") future_flg = 'C';
      string("\x01") cag_override_data_path= NULL("");
      decimal("\x01")[int] qual_output_profile_ids ;
      record
        string("\x01") qual_output_profile_name ;
        string("\x01") rxclaim_env_name;
        decimal("\x01") [int] job_ids;
        string("\x01") [int] alias_names;
        string("\x01") [int] job_names;
        string("\x01") [int] formulary_names ;
        string("\x01") [int] form_override_data_paths;
        string("\x01", maximum_length=30)[int] formulary_pseudonyms;
        string("\x01", maximum_length=20) tal_name ;
        string("\x01") tac_name ;
        string("\x01") tar_name ;
        string("\x01") tsd_name ;
        string("\x01", maximum_length=1) st_tac_ind ;
      end[int] op_dtls;
      record
        decimal("\x01") non_qual_op_id;
        string("\x01") rxclaim_env_name;
        decimal("\x01") [int] job_ids;
        string("\x01")[int] formulary_names;
        string("\x01", maximum_length=30)[int] formulary_pseudonyms;
        string("\x01") [int] alias_names;
        string("\x01") [int] job_names;
      end[int] non_qual_output_profile_ids ;
      string("\x01") [int] err_msgs;
      date("YYYYMMDD")("\x01") as_of_dt = NULL("");
      string(1) newline = "\n";
    end;
    type tar_dtl_t =
    record
      decimal("\x01",0, maximum_length=10) tar_id ;
      decimal("\x01",0, maximum_length=10) tar_dtl_id ;
      string("\x01", maximum_length=20) tar_name ;
      string("\x01", maximum_length=1) sort_ind ;
      string("\x01", maximum_length=1) filter_ind ;
      decimal("\x01",0, maximum_length=39) priority ;
      decimal("\x01",0, maximum_length=39) tar_dtl_type_cd ;
      decimal("\x01",0, maximum_length=10) tar_roa_df_set_id = NULL("") ;
      string("\x01", maximum_length=4000) target_rule = NULL("") ;
      string("\x01", maximum_length=4000) alt_rule = NULL("") ;
      string("\x01", maximum_length=15) rebate_elig_cd = NULL("") ;
      date("YYYYMMDD")("\x01") eff_dt ;
      date("YYYYMMDD")("\x01") term_dt ;
      string(1) newline = "\n";
    end;
    type tar_roa_df_set_dtl_t =
    record
      decimal("\x01",0, maximum_length=10) tar_roa_df_set_dtl_id ;
      decimal("\x01",0, maximum_length=10) tar_roa_df_set_id ;
      string("\x01", maximum_length=2) target_roa_cd = NULL("") ;
      string("\x01", maximum_length=4) target_dosage_form_cd = NULL("") ;
      string("\x01", maximum_length=2) alt_roa_cd = NULL("") ;
      string("\x01", maximum_length=4) alt_dosage_form_cd = NULL("") ;
      decimal("\x01",0, maximum_length=39) priority ;
      string(1) newline = "\n";
    end;
    type tar_rule_xwalk_t =
    record
      decimal("\x01",0, maximum_length=10) tar_id ;
      decimal("\x01",0, maximum_length=10) tar_dtl_id ;
      string("\x01", maximum_length=20) tar_name ;
      decimal("\x01",0, maximum_length=10) tar_roa_df_set_id = NULL("");
      decimal("\x01",0, maximum_length=39) tar_dtl_type_cd ;
      decimal("\x01",0, maximum_length=39) priority ;
      string("\x01", maximum_length=1) sort_ind ;
      string("\x01", maximum_length=1) filter_ind ;
      string("\x01", maximum_length=15) rebate_elig_cd = NULL("") ;
      date("YYYYMMDD")("\x01") eff_dt ;
      date("YYYYMMDD")("\x01") term_dt ;
      record
       string(int) qualifier_cd;
       string(int) operator;
       string(int) compare_value;
       string(1) conjunction_cd;
      end[int] target_rule_def;
      record
        string(int) qualifier_cd;
        string(int) operator;
        string(int) compare_value;
        string(1) conjunction_cd;
      end[int] alt_rule_def;
      string(1) newline = "\n";
    end;
    type tar_xwalk_t =
    record  
      decimal("\x01",0, maximum_length=10) tar_id ;
      decimal("\x01",0, maximum_length=10) tar_dtl_id ;
      string("\x01", maximum_length=20) tar_name ;
      decimal("\x01",0, maximum_length=39) tar_dtl_type_cd ;
      record
        bit_vector_t[int] target_prdcts;
        record
          bit_vector_t[int] alt_prdcts;
          bit_vector_t[int][int] alt_prdcts_all_prio;
          bit_vector_t common_prdcts;
        end[int] alt_contents;
      end[int] contents;
      bit_vector_t common_alt_prdcts;
      bit_vector_t common_target_prdcts;
      decimal(1) keep_all_targets = 0;
      string(1) newline = '\n';
    end;
    type alt_run_alt_dtl_load_t =
    record
      decimal("\x01",0, maximum_length=16 ) alt_run_alt_dtl_id = -1 ;
      decimal("\x01",0, maximum_length=16 ) alt_run_id = -1;
      decimal("\x01",0, maximum_length=16 ) alt_run_target_dtl_id = -1 ;
      string("\x01", maximum_length=20) formulary_name ;
      string("\x01", maximum_length=11) target_ndc ;
      string("\x01", maximum_length=11) alt_ndc ;
      string("\x01", maximum_length=2) alt_formulary_tier ;
      string("\x01", maximum_length=1) alt_multi_src_cd ;
      string("\x01", maximum_length=2) alt_roa_cd ;
      string("\x01", maximum_length=4) alt_dosage_form_cd ;
      decimal("\x01",0, maximum_length=39 ) rank ;
      string("\x01", maximum_length=1) alt_step_therapy_ind ;
      string("\x01", maximum_length=1) alt_pa_reqd_ind ;
      string("\x01", maximum_length=2) alt_formulary_status ;
      string("\x01", maximum_length=1) alt_specialty_ind ;
      string("\x01", maximum_length=14) alt_gpi14 ;
      string("\x01", maximum_length=70) alt_prod_name_ext ;
      string("\x01", maximum_length=30) alt_prod_short_desc ;
      string("\x01", maximum_length=30) alt_prod_short_desc_grp;
      string("\x01", maximum_length=60) alt_gpi14_desc ;
      string("\x01", maximum_length=60) alt_gpi8_desc ;
      string("\x01", maximum_length=50) alt_formulary_tier_desc ;
      string("\x01", maximum_length=50) alt_formulary_status_desc ;
      string("\x01", maximum_length=1) alt_pa_type_cd ;
      string("\x01", maximum_length=1) alt_step_therapy_type_cd ;
      string("\x01", maximum_length=1000) alt_step_therapy_group_name = NULL("") ;
      string("\x01", maximum_length=100) alt_step_therapy_step_number = NULL("") ;
      datetime("YYYY-MM-DD HH24:MI:SS")("\x01") rec_crt_ts ;
      string("\x01", maximum_length=30) rec_crt_user_id ;
      string("\x01", maximum_length=15) rebate_elig_cd = NULL("") ;
      string("\x01", maximum_length=15) tad_eligible_cd= NULL("");
      decimal("\x01".3, maximum_length=7) alt_qty_adj = NULL("") ;
      string("\x01", maximum_length=40) tal_assoc_name = NULL("") ;
      string("\x01", maximum_length=40) tala = NULL("") ;
      string("\x01", maximum_length=20) alt_udl = NULL("") ;
      decimal("\x01".6, maximum_length=39) tal_assoc_rank ;
      string("\x01", maximum_length=2) constituent_group = NULL("") ;
      string("\x01", maximum_length=1) constituent_reqd = NULL("") ;
      string(1) newline = "\n";
    end;
    type alt_run_target_dtl_load_t =
    record
      decimal("\x01",0, maximum_length=16 ) alt_run_target_dtl_id = -1 ;
      decimal("\x01",0, maximum_length=16 ) alt_run_id  = -1 ;
      string("\x01", maximum_length=40) tal_assoc_name = NULL("") ;
      string("\x01", maximum_length=40) tala = NULL("") ;
      string("\x01", maximum_length=20) tar_udl = NULL("") ;
      string("\x01", maximum_length=20) formulary_name ;
      string("\x01", maximum_length=11) target_ndc ;
      string("\x01", maximum_length=2) target_formulary_tier ;
      string("\x01", maximum_length=2) target_formulary_status ;
      string("\x01", maximum_length=1) target_pa_reqd_ind ;
      string("\x01", maximum_length=1) target_step_therapy_ind ;
      string("\x01", maximum_length=1) target_specialty_ind ;
      string("\x01", maximum_length=1) target_multi_src_cd ;
      string("\x01", maximum_length=2) target_roa_cd ;
      string("\x01", maximum_length=4) target_dosage_form_cd ;
      string("\x01", maximum_length=14) target_gpi14 ;
      string("\x01", maximum_length=70) target_prod_name_ext ;
      string("\x01", maximum_length=30) target_prod_short_desc ;
      string("\x01", maximum_length=60) target_gpi14_desc ;
      string("\x01", maximum_length=60) target_gpi8_desc ;
      string("\x01", maximum_length=50) target_formulary_tier_desc ;
      string("\x01", maximum_length=50) target_formulary_status_desc ;
      string("\x01", maximum_length=1) target_pa_type_cd ;
      string("\x01", maximum_length=1) target_step_therapy_type_cd ;
      string("\x01", maximum_length=1000) target_step_therapy_group_name = NULL("") ;
      decimal("\x01",0, maximum_length=39) target_step_therapy_step_num = NULL("") ;
      string("\x01", maximum_length=10) formulary_cd = NULL("") ;
      datetime("YYYY-MM-DD HH24:MI:SS")("\x01") rec_crt_ts ;
      string("\x01", maximum_length=30) rec_crt_user_id ;
      date("YYYYMMDD")("\x01") last_exp_dt ;
      string(1) newline = "\n";
    end;
    type alt_run_load_t =
    record
      decimal("\x01",0, maximum_length=16) alt_run_id = -1 ;
      decimal("\x01",0, maximum_length=10) output_profile_id ;
      datetime("YYYY-MM-DD HH24:MI:SS")("\x01") run_start_ts = NULL("") ;
      datetime("YYYY-MM-DD HH24:MI:SS")("\x01") run_complete_ts = NULL("") ;
      decimal("\x01",0, maximum_length=39) alt_run_status_cd ;
      string("\x01", maximum_length=1) published_ind ;
      datetime("YYYY-MM-DD HH24:MI:SS")("\x01") rec_crt_ts ;
      string("\x01", maximum_length=30) rec_crt_user_id ;
      datetime("YYYY-MM-DD HH24:MI:SS")("\x01") rec_last_upd_ts ;
      string("\x01", maximum_length=30) rec_last_upd_user_id ;
      date("YYYYMMDD")("\x01") run_eff_dt ;
      date("YYYYMMDD")("\x01") as_of_dt = NULL("") ;
      string(1) newline = "\n";
    end;
    type alt_run_load_adhoc_t =
    record
      decimal("\x01",0, maximum_length=16) alt_run_id = -1 ;
      decimal("\x01",0, maximum_length=10) output_profile_id ;
      datetime("YYYY-MM-DD HH24:MI:SS")("\x01") run_start_ts = NULL("") ;
      datetime("YYYY-MM-DD HH24:MI:SS")("\x01") run_complete_ts = NULL("") ;
      decimal("\x01",0, maximum_length=39) alt_run_status_cd ;
      string("\x01", maximum_length=1) published_ind ;
      datetime("YYYY-MM-DD HH24:MI:SS")("\x01") rec_crt_ts ;
      string("\x01", maximum_length=30) rec_crt_user_id ;
      datetime("YYYY-MM-DD HH24:MI:SS")("\x01") rec_last_upd_ts ;
      string("\x01", maximum_length=30) rec_last_upd_user_id ;
      date("YYYYMMDD")("\x01") run_eff_dt ;
      string(1) newline = "\n";
    end;
    type alt_run_t =
    record
      decimal("|") alt_run_id ;
      decimal("|") output_profile_id ;
      datetime("YYYY-MM-DD HH24:MI:SS")("|") run_start_ts = NULL("") ;
      datetime("YYYY-MM-DD HH24:MI:SS")("|") run_complete_ts = NULL("") ;
      decimal("|") alt_run_status_cd ;
      string("|") published_ind ;
      datetime("YYYY-MM-DD HH24:MI:SS")("|") rec_crt_ts ;
      string("|") rec_crt_user_id ;
      datetime("YYYY-MM-DD HH24:MI:SS")("|") rec_last_upd_ts ;
      string("|") rec_last_upd_user_id ;
      date("YYYYMMDD")("\n") run_eff_dt ;
    end;
    type alt_run_alt_proxy_dtl_load_t =
    record
      decimal("\x01",0, maximum_length=16 ) alt_run_id = -1;
      decimal("\x01",0, maximum_length=16 ) alt_run_alt_dtl_id = -1 ;
      decimal("\x01",0, maximum_length=16 ) alt_run_target_dtl_id = -1 ;
      string("\x01", maximum_length=20) formulary_name ;
      string("\x01", maximum_length=11) target_ndc ;
      string("\x01", maximum_length=40) tal_assoc_name ;
      string("\x01", maximum_length=11) alt_proxy_ndc ;
      decimal("\x01",0, maximum_length=39 ) rank ;
      datetime("YYYY-MM-DD HH24:MI:SS")("\x01") rec_crt_ts ;
      string("\x01", maximum_length=30) rec_crt_user_id ;
      string(1) newline = "\n";
    end;
    type file_load_cntl_load_t =
    record
      string("\x01", maximum_length=50) user_id ;
      string("\x01", maximum_length=1) file_load_type_cd ;
      decimal("\x01",0, maximum_length=39) component_type_cd = NULL("") ;
      decimal("\x01",0, maximum_length=39) file_load_status_cd ;
      datetime("YYYY-MM-DD HH24:MI:SS")("\x01") file_load_start_ts = NULL("") ;
      datetime("YYYY-MM-DD HH24:MI:SS")("\x01") rec_crt_ts ;
      string("\x01", maximum_length=30) rec_crt_user_id ;
      string("\x01", maximum_length=1) published_ind ;
      decimal("\x01",0, maximum_length=16) alt_run_id = NULL("") ;
      string(1) newline = "\n";
    end;
    type file_load_ctl_dtl_t =
    record
      decimal("\x01",0, maximum_length=16) file_load_cntl_id ;
      string("\x01", maximum_length=4000) component_ids = NULL("") ;
      date("YYYYMMDD")("\x01") as_of_date ;
      string("\x01", maximum_length=20) rxclaim_env_name = NULL("") ;
      string("\x01", maximum_length=20) carrier = NULL("") ;
      string("\x01", maximum_length=20) account = NULL("") ;
      string("\x01", maximum_length=20) group = NULL("") ;
      decimal("\x01",0, maximum_length=39) component_type_cd = NULL("") ;
      string("\x01", maximum_length=512) file_name_w = NULL("") ;
      string("\x01", maximum_length=1) published_ind ;
      decimal("\x01",0, maximum_length=16) alt_run_id = NULL("") ;
      string("\x01", maximum_length=512) report_file_name = NULL("") ;
      string(1) newline = "\n";
    end;
    type file_load_err_dtl_load_t =
    record
      decimal(",",0, maximum_length=16) file_load_cntl_id ;
      string(",", maximum_length=1024) err_desc ;
      datetime("YYYY-MM-DD HH24:MI:SS")(",") rec_crt_ts ;
      string(",", maximum_length=30) rec_crt_user_id ;
      datetime("YYYY-MM-DD HH24:MI:SS")(",") rec_last_upd_ts ;
      string("\n", maximum_length=30) rec_last_upd_user_id ;
    end;
    type tal_assoc_prdcts_t =
    record
      decimal("\x01",0, maximum_length=10) tal_assoc_dtl_id ;
      decimal("\x01",0, maximum_length=10) tal_assoc_id ;
      string("\x01", maximum_length=20) tal_assoc_name ;
      decimal("\x01",0, maximum_length=39) tal_assoc_type_cd ;
      bit_vector_t[int] target_prdcts;
      bit_vector_t[int] alt_prdcts;
      string("\x01", maximum_length=30) shared_qual ;
      string("\x01", maximum_length=20) override_tac_name = NULL("") ;
      string("\x01", maximum_length=20) override_tar_name = NULL("") ;
      string(1) newline = "\n";
    end;
    type part_exp_master_file_t =
    record
      decimal("\x01",0, maximum_length=16) file_load_cntl_id ;
      string("\x01", maximum_length=1024) component_ids ;
      date("YYYYMMDD")("\x01") as_of_date ;
      string("\x01", maximum_length=20) rxclaim_env_name = "" ;
      string("\x01", maximum_length=50) cag_in = NULL("") ;
      string("\x01", maximum_length=50) cag_out = NULL("") ;  
      decimal("\x01",0, maximum_length=39, sign_reserved) component_type_cd = NULL("") ;
      utf8 string("\x01", maximum_length=512) file_name_w = NULL("") ;
      utf8 string("\x01", maximum_length=1) published_ind ;
      string(1) newline = "\n";
    end;
    type alt_rank_t =
    record
      int alt_rank; 
      int[int] alt_prdcts;
      string(2) constituent_group = NULL("");
      string(1) constituent_reqd = NULL("");
      string(20) udl_nm = NULL("");
    end;
    type rebate_elig_contents_t =
    record
      string("\x01") rebate_elig_cd;
      int[int] rebate_elig_prdcts;
    end;
    type target_prdct_t =
    record
      int target_dl_bit;
      decimal('\x01') bucket_index = 0;
      decimal('\x01') udl_index = 0;
      decimal('\x01') tac_index = 0;
      alt_rank_t[int] alt_prdcts_rank;
      string("\x01")has_alt;
      decimal(1) ST_flag = 0;
    end;
    type tal_assoc_adhoc_t =
    record
      decimal("\x01",0, maximum_length=10) tal_assoc_dtl_id ;
      decimal("\x01",0, maximum_length=10) tal_assoc_id ;
      string("\x01", maximum_length=20) tal_assoc_name ;
      string("\x01", maximum_length=2000) clinical_indn_desc = NULL("");
      string("\x01", maximum_length=60) tal_assoc_desc ;
      decimal("\x01",0, maximum_length=39) tal_assoc_type_cd ;
      record
        string("\x01", maximum_length=20) udl_nm;
        string("\x01", maximum_length=60) udl_desc; 
        bit_vector_t products;
      end[int] target_udl_info ;
      record
         string("\x01", maximum_length=20) udl_nm;
         string("\x01", maximum_length=60) udl_desc; 
         bit_vector_t products;
         string(2) constituent_group = NULL("");
         string(1) constituent_reqd = NULL("");
         decimal("\x01",0, maximum_length=39) constituent_rank = NULL("");
      end[int] alt_udl_info;
      string("\x01", maximum_length=30) shared_qual ;
      string("\x01", maximum_length=20) override_tac_name = NULL("") ;
      string("\x01", maximum_length=20) override_tar_name = NULL("") ;
      string(1) newline = "\n";
    end;
    type tal_container_adhoc_t =
    record
      decimal("\x01",0, maximum_length=10) tal_id ;
      string("\x01", maximum_length=20) tal_name ;
      string("\x01", maximum_length=20) tal_assoc_name = NULL("") ;
      string("\x01", maximum_length=2000) clinical_indn_desc = NULL("");
      string("\x01", maximum_length=60) tal_desc ;
      string("\x01", maximum_length=60) tal_assoc_desc ;
      decimal("\x01", 6, maximum_length=39) priority ;
      decimal("\x01",0, maximum_length=39) tal_assoc_type_cd ;
      record
          string("\x01", maximum_length=20) udl_nm;
          string("\x01", maximum_length=60) udl_desc;
          bit_vector_t products;
      end[int] target_prdcts;
      record
          string("\x01", maximum_length=20) udl_nm;
          string("\x01", maximum_length=60) udl_desc;
          bit_vector_t products;
          string(2) constituent_group = NULL("");
          string(1) constituent_reqd = NULL("");
          decimal("\x01",0, maximum_length=39) constituent_rank = NULL("");
      end[int] alt_prdcts;
      string("\x01", maximum_length=30) shared_qual ;
      string("\x01", maximum_length=20) override_tac_name = NULL("") ;
      string("\x01", maximum_length=20) override_tar_name = NULL("") ;
      string(1) newline = "\n";
    end;
    type tal_assoc_adhoc_enrich_t =
    record
      decimal("\x01",0, maximum_length=10) tal_assoc_dtl_id ;
      decimal("\x01",0, maximum_length=10) tal_assoc_id ;
      string("\x01", maximum_length=20) tal_assoc_name ;
      string("\x01", maximum_length=2000) clinical_indn_desc = NULL("");
      string("\x01", maximum_length=60) tal_assoc_desc ;
      decimal("\x01",0, maximum_length=39) tal_assoc_type_cd ;
      string("\x01", maximum_length=20) udl_id = NULL("") ;  
      string("\x01", maximum_length=60) udl_desc; 
      string("\x01", maximum_length=20) Target_Alternative;
      bit_vector_t products;
      decimal("\x01",0, maximum_length=39) alt_rank = NULL("") ;
      string("\x01", maximum_length=30) shared_qual ;
      string("\x01", maximum_length=20) override_tac_name = NULL("") ;
      string("\x01", maximum_length=20) override_tar_name = NULL("") ;
      string(2) constituent_group = NULL("");
      string(1) constituent_reqd = NULL("");
      decimal("\x01",0, maximum_length=39) constituent_rank = NULL("");
      string(1) newline = "\n";
    end;
    type tal_container_adhoc_enrich_t =
    record
      string("\x01", maximum_length=20) tal_id ;
      string("\x01", maximum_length=20) tal_assoc_id = NULL("") ;
      string("\x01", maximum_length=2000) clinical_indn_desc = NULL("");
      string("\x01", maximum_length=60) tal_desc ;
      string("\x01", maximum_length=60) tal_assoc_desc ;
      decimal("\x01",0, maximum_length=39) tal_assoc_type_cd ;
      decimal("\x01", 6, maximum_length=39) tal_assoc_rank = NULL("") ;
      string("\x01", maximum_length=20) udl_id = NULL("") ;  
      string("\x01", maximum_length=60) udl_desc; 
      string("\x01", maximum_length=20) Target_Alternative;
      bit_vector_t products;
      decimal("\x01",0, maximum_length=39) alt_rank = NULL("") ;
      string("\x01", maximum_length=30) shared_qual ;
      string("\x01", maximum_length=20) override_tac_name = NULL("") ;
      string("\x01", maximum_length=20) override_tar_name = NULL("") ;
      string(2) constituent_group = NULL("");
      string(1) constituent_reqd = NULL("");
      decimal("\x01",0, maximum_length=39) constituent_rank = NULL("");
      string(1) newline = "\n";
    end;
    type error_info_V2_16 = record 
      utf8 string(big endian integer(4)) component;
      big endian integer(4) port_index;
      utf8 string(big endian integer(4)) parameter;
      utf8 string(big endian integer(4)) message;
      record
        utf8 string(big endian integer(4)) code;
        big endian integer(4) parent_index;
        record
          utf8 string(big endian integer(4)) name;
          utf8 string(big endian integer(4)) value;
        end [big endian integer(4)] attributes;
      end [big endian integer(4)] details;
    end;
    type error_info_t = error_info_V2_16;
    type alt_run_target_dtl_ref_t =
    record
      decimal("\x01",0, maximum_length=16 ) alt_run_target_dtl_id ;
      string("\x01", maximum_length=40) tal_assoc_name ;
      string("\x01", maximum_length=20) formulary_name ;
      string("\x01", maximum_length=11) target_ndc ;
    end;
    type alt_run_alt_dtl_ref_t =
    record
      decimal("\x01",0, maximum_length=16 ) alt_run_alt_dtl_id ;
      decimal("\x01",0, maximum_length=16 ) alt_run_target_dtl_id ;
      string("\x01", maximum_length=20) formulary_name ;
      string("\x01", maximum_length=40) tal_assoc_name ;
      string("\x01", maximum_length=11) target_ndc ;
      string("\x01", maximum_length=11) alt_ndc ;
    end;
    type master_cag_mapping_adhoc_t =
    record
      string("\x01") carrier = NULL("") ;
      string("\x01") account = NULL("") ;
      string("\x01") group = NULL("") ;
      string("\x01") cag_override_data_path= NULL("");
      decimal("\x01")[int] output_profile_id ;
      record
        string("\x01") output_profile_name ;
        string("\x01") [int] alias_names;
        string("\x01") [int] job_names;
        string("\x01")[int] formulary_names ;
        string("\x01") [int] form_override_data_paths;
        string("\x01")[int] formulary_pseudonyms ;
        string("\x01") tal_name ;
        string("\x01") tac_name ;
        string("\x01") tar_name ;
        string("\x01") tsd_name ;
        string("\x01", maximum_length=1) st_tac_ind ;
      end[int] op_dtls;
      string("\x01")[int] err_msgs ;
      string(1) newline = "\n";
    end;
    type tal_expansion_test_harness =
    record
      string('\x01') tal_id;
      string('\x01') tal_assoc_id;
      string('\x01') tal_assoc_desc;
      string('\x01') tal_assoc_type_cd;
      string('\x01') override_tac_name;
      string('\x01') override_tar_name;
      string('\x01') shared_qual;
      string('\x01') tal_assoc_rank;
      string('\x01') udl_id;
      string('\x01') udl_desc;
      string('\x01') alt_rank;
      string('\x01') target_alternative;
      string('\x01') ndc11;
      string('\x01') gpi14;
      string('\x01') msc;
      string('\x01') drug_name;
      string('\x01') prod_short_desc;
      string('\x01') gpi14_desc;
      string('\x01') prod_strength;
      string('\x01') roa_cd;
      string('\x01') dosage_form_cd;
      string('\x01') rx_otc;
      string('\x01') repack_cd;
      string('\x01') status_cd;
      string('\x01') inactive_dt;
      string(1) newline = '\n';
    end;
    type udl_expansion_test_harness =
    record  
      string('\x01')udl_nm ;   
      string('\x01')ndc11 ;
      string('\x01')gpi14 ;
      string('\x01')msc ;
      string('\x01')drug_name ;
      string('\x01')prod_short_desc ;
      string('\x01')gpi14_desc ;
      string('\x01')prod_strength;  
      string('\x01')roa_cd ;
      string('\x01')dosage_form_cd ;
      string('\x01')rx_otc ;
      string('\x01')repack_cd;  
      string('\x01')status_cd ; 
      string('\x01')inactive_dt ; 
      string(1)newline = '\n';
    end;
    type alt_run_job_details_load_t = 
    record
      decimal("\x01",0, maximum_length=16) alt_run_id ;
      decimal("\x01",0, maximum_length=16) job_run_id ;
      datetime("YYYY-MM-DD HH24:MI:SS")("\x01") rec_crt_ts ;
      string("\x01", maximum_length=30) rec_crt_user_id ;
      string(1) newline = "\n";
    end;
    type alias_dtl_t = 
    record
      decimal("\x01",0, maximum_length=10) alias_dtl_id ;
      decimal("\x01",0, maximum_length=10) alias_id ;
      string("\x01", maximum_length=20) alias_name ;
      decimal("\x01",0, maximum_length=39) qual_priority = 99;
      string("\x01", maximum_length=10) qual_id_type_cd = NULL("") ;
      string("\x01", maximum_length=14) qual_id_value = NULL("") ;
      string("\x01", maximum_length=70) search_txt ;
      string("\x01", maximum_length=70) replace_txt = NULL("") ;
      decimal("\x01",0, maximum_length=39) rank ;
      date("YYYYMMDD")("\x01") eff_dt ;
      date("YYYYMMDD")("\x01") term_dt ;
      string(1) newline = "\n";
    end;
    type alias_info_t =  record
            decimal("\x01",0, maximum_length=39) qual_priority = 99;
            string("\x01", maximum_length=14) qual_id_value = NULL("") ;
            string("\x01", maximum_length=70) search_txt ;
            string("\x01", maximum_length=70) replace_txt  ;
            date("YYYYMMDD")("\x01") eff_dt;
            date("YYYYMMDD")("\x01") term_dt;
    end;
    type alias_xwalk_t = 
    record
      decimal("\x01",0, maximum_length=10) alias_id ;
      string("\x01", maximum_length=20) alias_name ;
      alias_info_t[int] alias_info; 
    end;
    type target_alt_rank_t =
    record
        int target_dl_bit;
        decimal('\x01') bucket_index = 0;
        decimal('\x01') udl_index = 0;
        string(20) tar_udl_nm =NULL("");
        decimal('\x01') tac_index = 0;
        record
          int alt_rank;
          int[int] alt_prdcts;
          string(2) constituent_group = NULL("");
          string(1) constituent_reqd = NULL("");
          string(20) udl_nm =NULL("");
        end[int] alt_prdcts_rank;   
        string("\x01")has_alt;
        decimal(1) ST_flag =0;
    end;
    type alias_content_xwalk_t =
    record
      string("\x01", maximum_length=13) alias_set_name;
      string("\x01", maximum_length=11)  ndc11 ;
      string("\x01") alias_label_nm ;
      string(1) newline = "\n";
    end;
    type gpi_rank_ratio_t =
    record
      string("\x01", maximum_length=14) gpi14 ;
      decimal("\x01",0, maximum_length=39) rank ;
      decimal("\x01".3, maximum_length=6) ratio = NULL("") ;
      date("YYYYMMDD")("\x01") run_eff_dt = NULL("") ;
     end;
    type tad_t =
    record
      decimal("\x01",0, maximum_length=10) tad_id ;
      string("\x01", maximum_length=14) target_gpi14 ;
      string("\x01", maximum_length=12) alt_grouping_gpi12 ;
      string("\x01", maximum_length=14) alt_selection_id ;
      decimal("\x01",0, maximum_length=39) rank ;
      decimal("\x01".3, maximum_length=7) qty_adjust_factor = NULL("") ;
      string(1) newline = "\n";
    end;
    type tad_xwalk_t =
    record
      string("\x01", maximum_length=14) target_gpi14 ;
      string("\x01", maximum_length=6) alt_selection_cd;
      string("\x01", maximum_length=14)[int] alt_selection_ids ;
      record
        decimal("\x01",0, maximum_length=39) rank ;
        decimal("\x01".3, maximum_length=7) qty_adjust_factor = NULL("") ;
      end[int] tad_alt_dtls ;
      string(1) newline = "\n";
    end;
    type step_grp_xwalk_t =
    record
      string("\x01", maximum_length=11) ndc11 ;
      decimal("\x01",0, maximum_length=39) step_therapy_step_number = NULL("") ;
      string("\x01", maximum_length=100)[int] step_therapy_group_names  ;
    end;
    type step_grp_num_xwalk_t =
    record
      decimal("\x01",0, maximum_length=10) tac_id ;
      string("\x01", maximum_length=20) tac_name ;
      decimal("\x01",0, maximum_length=39) priority ;
      decimal("\x01") _target_st_grp_num = allocate();
      decimal("\x01")[int] _alt_st_grp_nums = allocate();
    end;
    type tal_assoc_clinical_indn_t =
    record
     decimal("\x01",0, maximum_length=10) tal_assoc_id ;
     string("\x01", maximum_length=40) tal_assoc_name ;
     decimal("\x01",0, maximum_length=10) clinical_indn_id = NULL("");
     string("\x01", maximum_length=20) clinical_indn_name ;
     string("\x01", maximum_length=200) clinical_indn_desc = NULL("");
     decimal("\x01",0, maximum_length=39) rank = NULL("");
    end;
    type alt_run_clinical_indn_dtl_load_t =
    record
      decimal("\x01",0, maximum_length=16) alt_run_id = -1 ;
      decimal("\x01",0, maximum_length=16) alt_run_target_dtl_id = -1 ;
      string("\x01", maximum_length=20) formulary_name ;
      string("\x01", maximum_length=11) target_ndc ;
      string("\x01", maximum_length=40) tal_assoc_name ;
      decimal("\x01",0, maximum_length=39) rank ;
      datetime("YYYY-MM-DD HH24:MI:SS")("\x01") rec_crt_ts ;
      string("\x01", maximum_length=30) rec_crt_user_id ;
      string("\x01", maximum_length=200) clinical_indn_desc = NULL("") ;
      string(1) newline = "\n";
    end;
    type form_target_reject_t = 
    record
    string(",") error_msg;
    decimal(",") tal_id ;
    string(",") tal_name ;
    string(",") tal_assoc_name = NULL("") ;
    string(",") ndc11 = NULL("");
    string(",") gpi14 = NULL("");
    string("\n") prod_short_desc = NULL("");
    end;
    type form_alt_reject_t = 
    record
    string(",") error_msg;
    decimal(",") tal_id ;
    string(",") tal_name ;
    string(",") tal_assoc_name = NULL("") ;
    string(",") target_ndc11 = NULL("");
    string(",") alt_ndc11 = NULL("");
    string(",") gpi14 = NULL("");
    string("\n") prod_short_desc = NULL("");
    end;
    type tal_assoc_enriched_data_t = 
    record
      string("\x01", maximum_length=20) tal_assoc_name ;
      decimal("\x01",0, maximum_length=39) tal_assoc_type_cd ;
      string("\x01", maximum_length=20) Target_Alternative;
      string("\x01", maximum_length=11)  ndc11 ;
      string("\x01", maximum_length=14)  gpi14 ;
      string("\x01", maximum_length=1)  msc ;
      string("\x01", maximum_length=70)  drug_name ;
      string("\x01", maximum_length=30) prod_short_desc ;
      string("\x01", maximum_length=60) gpi14_desc ;
      string("\x01", maximum_length=2)  roa_cd ;
      string("\x01", maximum_length=4)  dosage_form_cd ;
      string("\x01", maximum_length=3)  rx_otc ;
      string("\x01", maximum_length=1)  repack_cd ; 
      string("\x01", maximum_length=1)  status_cd ;
      string("\x01", maximum_length=8) inactive_dt ;
      decimal("\x01",0, maximum_length=39) alt_rank = NULL("") ;
      string("\x01", maximum_length=30) shared_qual ;
      string("\x01", maximum_length=20) override_tac_name = NULL("") ;
      string("\x01", maximum_length=20) override_tar_name = NULL("") ;
      string(2) constituent_group = NULL("");
      string(1) constituent_reqd = NULL("");
      decimal("\x01",0, maximum_length=39) constituent_rank = NULL("");
      string(1) newline = "\n";
    end;
    type tsd_enriched_data_t =
    record
      decimal("\x01",0, maximum_length=10) tsd_id ;
      string("\x01", maximum_length=30) tsd_cd ;
      string("\x01", maximum_length=11)  ndc11 ;
      string("\x01", maximum_length=14)  gpi14 ;
      string("\x01", maximum_length=1)  msc ;
      string("\x01", maximum_length=70)  drug_name ;
      string("\x01", maximum_length=30) prod_short_desc ;
      string("\x01", maximum_length=60) gpi14_desc ;
      string("\x01", maximum_length=2)  roa_cd ;
      string("\x01", maximum_length=4)  dosage_form_cd ;
      string("\x01", maximum_length=3)  rx_otc ;
      string("\x01", maximum_length=1)  repack_cd ; 
      string("\x01", maximum_length=1)  status_cd ;
      string("\x01", maximum_length=8) inactive_dt ;
      string(1) newline = "\n";
    end;
    type tac_enriched_data_t =
    record
      decimal("\x01",0, maximum_length=10) tac_id ;
      string("\x01", maximum_length=20) tac_name ;
      decimal("\x01",0, maximum_length=39) priority ;
      string("\x01", maximum_length=20) Target_Alternative;
      string("\x01", maximum_length=11)  ndc11 ;
      string("\x01", maximum_length=14)  gpi14 ;
      string("\x01", maximum_length=1)  msc ;
      string("\x01", maximum_length=70)  drug_name ;
      string("\x01", maximum_length=30) prod_short_desc ;
      string("\x01", maximum_length=60) gpi14_desc ;
      string("\x01", maximum_length=2)  roa_cd ;
      string("\x01", maximum_length=4)  dosage_form_cd ;
      string("\x01", maximum_length=3)  rx_otc ;
      string("\x01", maximum_length=1)  repack_cd ; 
      string("\x01", maximum_length=1)  status_cd ;
      string("\x01", maximum_length=8) inactive_dt ;
      string(1) newline = "\n";
    end;
    type tar_enriched_data_t = 
    record
      decimal("\x01",0, maximum_length=10) tar_id  ;
      string("\x01", maximum_length=20) tar_name ;
      string("\x01", maximum_length=20) Target_Alternative;
      string("\x01", maximum_length=11)  ndc11 ;
      string("\x01", maximum_length=14)  gpi14 ;
      string("\x01", maximum_length=1)  msc ;
      string("\x01", maximum_length=70)  drug_name ;
      string("\x01", maximum_length=30) prod_short_desc ;
      string("\x01", maximum_length=60) gpi14_desc ;
      string("\x01", maximum_length=2)  roa_cd ;
      string("\x01", maximum_length=4)  dosage_form_cd ;
      string("\x01", maximum_length=3)  rx_otc ;
      string("\x01", maximum_length=1)  repack_cd ; 
      string("\x01", maximum_length=1)  status_cd ;
      string("\x01", maximum_length=8) inactive_dt ;
      string(1) newline = "\n";
    end;
    type asso_enriched_tac_data_t =
    record
      decimal("\x01",0, maximum_length=10) tal_id ;
      string("\x01", maximum_length=20) tal_name ;
      string("\x01", maximum_length=20) tal_assoc_name = NULL("") ;
      string("\x01", maximum_length=20) Target_Alternative;
      string("\x01", maximum_length=11)  ndc11 ;
      string("\x01", maximum_length=14)  gpi14 ;
      string("\x01", maximum_length=1)  msc ;
      string("\x01", maximum_length=70)  drug_name ;
      string("\x01", maximum_length=30) prod_short_desc ;
      string("\x01", maximum_length=60) gpi14_desc ;
      string("\x01", maximum_length=2)  roa_cd ;
      string("\x01", maximum_length=4)  dosage_form_cd ;
      string("\x01", maximum_length=3)  rx_otc ;
      string("\x01", maximum_length=1)  repack_cd ; 
      string("\x01", maximum_length=1)  status_cd ;
      string("\x01", maximum_length=8) inactive_dt ;
      string("\x01", maximum_length=30) shared_qual ;
      string("\x01", maximum_length=20) override_tac_name = NULL("") ;
      string("\x01", maximum_length=20) override_tar_name = NULL("") ;
      string(1) newline = "\n";
    end;
    type asso_enriched_sorted_data_t =
    record
      decimal("\x01",0, maximum_length=10) tal_id ;
      string("\x01", maximum_length=20) tal_name ;
      string("\x01", maximum_length=20) tal_assoc_name = NULL("") ;
      string("\x01", maximum_length=11)  Target_ndc11 ='N/A';
      string("\x01", maximum_length=20) Target_Alternative;
      string("\x01", maximum_length=11)  ndc11 ;
      string("\x01", maximum_length=15) tad_eli_code= NULL("") ;
      string("\x01", maximum_length=14)  gpi14 ;
      string("\x01", maximum_length=1)  msc ;
      string("\x01", maximum_length=70)  drug_name ;
      string("\x01", maximum_length=30) prod_short_desc ;
      string("\x01", maximum_length=60) gpi14_desc ;
      string("\x01", maximum_length=2)  roa_cd ;
      string("\x01", maximum_length=4)  dosage_form_cd ;
      string("\x01", maximum_length=3)  rx_otc ;
      string("\x01", maximum_length=1)  repack_cd ; 
      string("\x01", maximum_length=1)  status_cd ;
      string("\x01", maximum_length=8) inactive_dt ;
      string(1) newline = "\n";
    end;
    type asso_enriched_diff_data_t =
    record
      string("\x01", maximum_length=20) Target_Alternative;
      string("\x01", maximum_length=11)  ndc11 ;
      string("\x01", maximum_length=14)  gpi14 ;
      string("\x01", maximum_length=1)  msc ;
      string("\x01", maximum_length=70)  drug_name ;
      string("\x01", maximum_length=30) prod_short_desc ;
      string("\x01", maximum_length=60) gpi14_desc ;
      string("\x01", maximum_length=2)  roa_cd ;
      string("\x01", maximum_length=4)  dosage_form_cd ;
      string("\x01", maximum_length=3)  rx_otc ;
      string("\x01", maximum_length=1)  repack_cd ; 
      string("\x01", maximum_length=1)  status_cd ;
      string("\x01", maximum_length=8) inactive_dt ;
      end;
    type udl_ref_t =
    record
      date("YYYYMMDD")("\x01") as_of_dt ;
    end;
    type output_profile_rebate_dtl_t =
    record
      decimal("\x01",0, maximum_length=10) output_profile_rebate_dtl_id ;
      string("\x01", maximum_length=20) udl_name ;
      decimal("\x01",0, maximum_length=10) output_profile_id ;
      string("\x01", maximum_length=15) rebate_elig_cd ;
      string(1) newline = "\n";
    end;
    metadata type = alt_run_clinical_indn_dtl_load_t ;""",
        readFormat = "fixedFormat",
        joinWithInputDataframe = false
      )
    
      val out = fileRecordDF
    out
  }

}
