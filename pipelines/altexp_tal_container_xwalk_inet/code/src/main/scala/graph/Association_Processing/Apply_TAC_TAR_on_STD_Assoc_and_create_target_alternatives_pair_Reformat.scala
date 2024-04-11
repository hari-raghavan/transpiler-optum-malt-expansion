package graph.Association_Processing

import io.prophecy.libs._
import graph.Association_Processing.config.Context
import udfs.UDFs._
import udfs.PipelineInitCode._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Apply_TAC_TAR_on_STD_Assoc_and_create_target_alternatives_pair_Reformat {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    //
    // @TODO: Parse error: 'CustomParserError(1420:1,Parse Error: starting at
    // out::reformat(in)=
    // ^)'
    //
    // type qual_priority_t =
    // record
    //   string(int) qual;
    //   decimal("") priority;
    // end;
    // constant string(int)[int] OVERRIDE_QUALIFIERS = [vector 'DESI_CD', 'DOSAGE_FORM', 'MSC', 'ROA', 'RXOTC' ];
    // constant qual_priority_t[int] QUAL_PRIORITY =[vector [record  qual "NDC11"        priority "1"],
    //                                                       [record qual "NDC9"         priority "2"],
    //                                                       [record qual "NDC5"         priority "3"],
    //                                                       [record qual "GPI14"        priority "4"],
    //                                                       [record qual "GPI12"        priority "5"],
    //                                                       [record qual "GPI10"        priority "6"],
    //                                                       [record qual "DOSAGE_FORM"  priority "7"],
    //                                                       [record qual "ROA"          priority "8"],
    //                                                       [record qual "DRUG_NAME"    priority "9"],
    //                                                       [record qual "GPI8"         priority "10"],
    //                                                       [record qual "GPI6"         priority "11"],
    //                                                       [record qual "GPI4"         priority "12"],
    //                                                       [record qual "MSC"          priority "13"],
    //                                                       [record qual "RXOTC"        priority "14"],
    //                                                       [record qual "DAYS_UNTIL_DRUG_STATUS_INACTIVE" priority "15"] ,
    //                                                       [record qual "STATUS_CD"  priority   "16"],
    //                                                       [record qual "REPACKAGER"   priority "17"],
    //                                                       [record qual "DESI_CD"    priority   "18"]];
    // type drug_data_set_dtl_t =
    // record
    //   string("\x01", maximum_length=20) rxclaim_env_name ;
    //   string("\x01", maximum_length=20) carrier = NULL("") ;
    //   string("\x01", maximum_length=20) account = NULL("") ;
    //   string("\x01", maximum_length=20) group = NULL("") ;
    //   date("YYYYMMDD")("\x01") run_eff_dt = NULL("") ;
    //   decimal("\x01",0, maximum_length=10) drug_data_set_dtl_id ;
    //   decimal("\x01",0, maximum_length=10) drug_data_set_id ;
    //   string("\x01", maximum_length=11) ndc11 ;
    //   string("\x01", maximum_length=14) gpi14 ;
    //   string("\x01", maximum_length=1) status_cd ;
    //   string("\x01", maximum_length=8) eff_dt = NULL("") ;
    //   string("\x01", maximum_length=8) term_dt = NULL("") ;
    //   string("\x01", maximum_length=8) inactive_dt = NULL("") ;
    //   string("\x01", maximum_length=1) msc ;
    //   string("\x01", maximum_length=70) drug_name ;
    //   string("\x01", maximum_length=30) prod_short_desc ;
    //   string("\x01", maximum_length=3) rx_otc ;
    //   string("\x01", maximum_length=1) rx_otc_cd ;
    //   string("\x01", maximum_length=1) desi ;
    //   string("\x01", maximum_length=2) roa_cd ;
    //   string("\x01", maximum_length=4) dosage_form_cd ;
    //   decimal("\x01".5, maximum_length=15) prod_strength ;
    //   string("\x01", maximum_length=1) repack_cd ;
    //   string("\x01", maximum_length=60) gpi14_desc ;
    //   string("\x01", maximum_length=60) gpi8_desc ;
    //   string(1) newline = "\n";
    // end;
    // type user_defined_list_rule_t =
    // record
    //   decimal("\x01",0, maximum_length=10) user_defined_list_id ;
    //   string("\x01", maximum_length=20) user_defined_list_name ;
    //   string("\x01", maximum_length=60) user_defined_list_desc ;
    //   decimal("\x01",0, maximum_length=10) user_defined_list_rule_id ;
    //   string("\x01", maximum_length=4000) rule = NULL("") ;
    //   string("\x01", maximum_length=1) incl_cd ;
    //   date("YYYYMMDD")("\x01") eff_dt ;
    //   date("YYYYMMDD")("\x01") term_dt ;
    //   string(1) newline = "\n";
    // end;
    // type rule_xwalk_t=
    // record
    //   decimal("\x01") udl_id;
    //   decimal("\x01") udl_rule_id;
    //   string("\x01") udl_nm;
    //   string("\x01") udl_desc;
    //   decimal("\x01") rule_priority;
    //   string(1) inclusion_cd;
    //   string(int) qualifier_cd;
    //   string(int) operator;
    //   string(int) compare_value;
    //   string(1) conjunction_cd;
    //   decimal("\x01") rule_expression_id;
    //   date("YYYYMMDD")("\x01") eff_dt ;
    //   date("YYYYMMDD")("\x01") term_dt ;
    //   string(1) newline = "\n";
    // end;
    // type udl_mstr_xwalk_t=
    // record
    //   decimal("\x01",0, maximum_length=10) udl_id;
    //   string("\x01", maximum_length=20) udl_nm;
    //   string("\x01", maximum_length=60) user_defined_list_desc;
    //   string(int)[int] qual_list;
    //   decimal(1) override_flg;
    //   date("YYYYMMDD")("\x01") eff_dt ;
    //   date("YYYYMMDD")("\x01") term_dt ;
    //   string(1) newline = "\n";
    // end;
    // type product_lkp_t =
    // record
    //   int dl_bit;
    //   string("\x01", maximum_length=11)  ndc11 ;
    //   string("\x01", maximum_length=14)  gpi14 ;
    //   string("\x01", maximum_length=1)  status_cd ;
    //   string("\x01", maximum_length=8) inactive_dt ;
    //   string("\x01", maximum_length=1)  msc ;
    //   string("\x01", maximum_length=70)  drug_name ;
    //   string("\x01", maximum_length=3)  rx_otc ;
    //   string("\x01", maximum_length=1)  desi ;
    //   string("\x01", maximum_length=2)  roa_cd ;
    //   string("\x01", maximum_length=4)  dosage_form_cd ;
    //   decimal("\x01".5, maximum_length=15) prod_strength ;
    //   string("\x01", maximum_length=1)  repack_cd ;
    //   string("\x01", maximum_length=30) prod_short_desc ;
    //   string("\x01", maximum_length=60) gpi14_desc ;
    //   string("\x01", maximum_length=60) gpi8_desc ;
    //   string(1) newline = "\n";
    // end;
    // type nrmlz_dataset_qual_t =
    // record
    //   int dl_bit;
    //   string(int) qualifier_cd;
    //   string(int) compare_value = NULL;
    // end;
    // type rule_product_lkp_t =
    // record
    //   string(int) qualifier_cd;
    //   string(int) operator;
    //   string(int) compare_value;
    //   bit_vector_t products;
    // end;
    // type cag_ovrrd_xwalk_t =
    // record
    //   string("\x01", maximum_length=20)  carrier = NULL("") ;
    //   string("\x01", maximum_length=20)  account = NULL("") ;
    //   string("\x01", maximum_length=20)  group = NULL("") ;
    //   string("\x01", maximum_length=11)  ndc11 ;
    //   string("\x01", maximum_length=14)  gpi14 ;
    //   string("\x01", maximum_length=1)  status_cd ;
    //   string("\x01", maximum_length=8) eff_dt = NULL("") ;
    //   string("\x01", maximum_length=8) term_dt = NULL("") ;
    //   string("\x01", maximum_length=8) inactive_dt ;
    //   string("\x01", maximum_length=1)  msc ;
    //   string("\x01", maximum_length=70)  drug_name ;
    //   string("\x01", maximum_length=3)  rx_otc ;
    //   string("\x01", maximum_length=1)  desi ;
    //   string("\x01", maximum_length=2)  roa_cd ;
    //   string("\x01", maximum_length=4)  dosage_form_cd ;
    //   decimal("\x01".5, maximum_length=15) prod_strength ;
    //   string("\x01", maximum_length=1)  repack_cd ;
    //   string("\x01", maximum_length=30) prod_short_desc ;
    //   string("\x01", maximum_length=60) gpi14_desc ;
    //   string("\x01", maximum_length=60) gpi8_desc ;
    //   string(1) newline = "\n";
    // end;
    // type cag_ovrrd_ref_file_t =
    // record
    //   string("\x01", maximum_length=20)  carrier = NULL("") ;
    //   string("\x01", maximum_length=20)  account = NULL("") ;
    //   string("\x01", maximum_length=20)  group = NULL("") ;
    //   decimal("\x01",0, maximum_length=1) is_future_snap = 0;
    //   string("\x01") data_path;
    //   string(1) newline = "\n";
    // end;
    // type udl_exp_t =
    // record
    //   decimal("\x01",0, maximum_length=10) udl_id;
    //   string("\x01", maximum_length=20) udl_nm;
    //   string("\x01", maximum_length=60) udl_desc;
    //   bit_vector_t products;
    //   date("YYYYMMDD")("\x01") eff_dt ;
    //   date("YYYYMMDD")("\x01") term_dt ;
    //   bit_vector_t[int] contents;
    //   string(1) newline = "\n";
    // end;
    // type tal_dtl_t =
    // record
    //   decimal("\x01",0, maximum_length=10) tal_dtl_id ;
    //   decimal("\x01",0, maximum_length=10) tal_id ;
    //   string("\x01", maximum_length=20) tal_name ;
    //   string("\x01", maximum_length=60) tal_desc ;
    //   decimal("\x01",0, maximum_length=39) tal_dtl_type_cd ;
    //   string("\x01", maximum_length=20) nested_tal_name = NULL("") ;
    //   string("\x01", maximum_length=20) tal_assoc_name = NULL("") ;
    //   decimal("\x01", 6, maximum_length=39) priority ;
    //   date("YYYYMMDD")("\x01") eff_dt ;
    //   date("YYYYMMDD")("\x01") term_dt ;
    //   string(1) newline = "\n";
    // end;
    // type tal_assoc_dtl_t =
    // record
    //   decimal("\x01",0, maximum_length=10) tal_assoc_dtl_id ;
    //   decimal("\x01",0, maximum_length=10) tal_assoc_id ;
    //   string("\x01", maximum_length=20) tal_assoc_name ;
    //   string("\x01", maximum_length=60) tal_assoc_desc ;
    //   decimal("\x01",0, maximum_length=39) tal_assoc_type_cd ;
    //   string("\x01", maximum_length=20) target_udl_name = NULL("") ;
    //   string("\x01", maximum_length=20) alt_udl_name = NULL("") ;
    //   decimal("\x01",0, maximum_length=39) alt_rank = NULL("") ;
    //   decimal("\x01",0, maximum_length=39) constituent_rank = NULL("") ;
    //   string("\x01", maximum_length=2) constituent_group = NULL("") ;
    //   string("\x01", maximum_length=1) constituent_reqd = NULL("") ;
    //   string("\x01", maximum_length=30) shared_qual ;
    //   string("\x01", maximum_length=20) override_tac_name = NULL("") ;
    //   string("\x01", maximum_length=20) override_tar_name = NULL("") ;
    //   string(1) newline = "\n";
    // end;
    // type tac_dtl_t =
    // record
    //   decimal("\x01",0, maximum_length=10) tac_dtl_id ;
    //   decimal("\x01",0, maximum_length=10) tac_id ;
    //   string("\x01", maximum_length=20) tac_name ;
    //   decimal("\x01",0, maximum_length=39) priority ;
    //   string("\x01", maximum_length=4000) target_rule = NULL("") ;
    //   string("\x01", maximum_length=4000) alt_rule = NULL("") ;
    //   date("YYYYMMDD")("\x01") eff_dt ;
    //   date("YYYYMMDD")("\x01") term_dt ;
    //   string(1) newline = "\n";
    // end;
    // type tac_rule_xwalk_t =
    // record
    //   decimal("\x01",0, maximum_length=10) tac_id ;
    //   string("\x01", maximum_length=20) tac_name ;
    //   decimal("\x01",0, maximum_length=39) priority ;
    //   date("YYYYMMDD")("\x01") eff_dt ;
    //   date("YYYYMMDD")("\x01") term_dt ;
    //   record
    //    string(int) qualifier_cd;
    //    string(int) operator;
    //    string(int) compare_value;
    //    string(1) conjunction_cd;
    //   end[int] target_rule_def;
    //   record
    //    string(int) qualifier_cd;
    //    string(int) operator;
    //    string(int) compare_value;
    //    string(1) conjunction_cd;
    //   end[int] alt_rule_def;
    //   string(1) newline = "\n";
    // end;
    // type contents_alt_st =
    // record
    //  bit_vector_t target_prdcts;
    //  bit_vector_t alternate_prdcts;
    //  string("\x01", maximum_length=40) tal_assoc_name;
    // end;
    // type tac_st_contents =
    // record
    //   decimal("\x01",0, maximum_length=10) tal_id;
    //   contents_alt_st[int] step_contents;
    //   string(1) newline = '\n';
    // end;
    // type contents_t =
    // record
    //   bit_vector_t target_prdcts;
    //   bit_vector_t alt_prdcts;
    //   decimal(1) ST_flag =0;
    //   decimal(1) priority;
    // end;
    // type tac_contents_t =
    // record
    //  bit_vector_t target_prdcts;
    //  bit_vector_t alt_prdcts;
    //  contents_t[int] contents;
    //  decimal(1) xtra_proc_flg = 0;
    // end;
    // type tac_xwalk_t =
    // record
    //   decimal("\x01",0, maximum_length=10) tac_id ;
    //   string("\x01", maximum_length=20) tac_name ;
    //   tac_contents_t[int] tac_contents;
    //   date("YYYYMMDD")("\x01") eff_dt ;
    //   date("YYYYMMDD")("\x01") term_dt ;
    //   string(1) newline = '\n';
    // end;
    // type tal_container_xwalk_t =
    // record
    //   string("\x01", maximum_length=20) tal_name ;
    //   int target_dl_bit;
    //   record
    //    int alt_prd;
    //    int alt_rank;
    //   end[int] alt_prdcts;
    //   string(1) newline = "\n";
    // end;
    // type target_udl_products_t =
    // record
    //   string("\x01", maximum_length=20) target_udl_name = NULL("") ;
    //   bit_vector_t target_products;
    // end;
    // type alt_udl_products_t =
    // record
    //   string("\x01", maximum_length=20) alt_udl_name = NULL("") ;
    //   decimal("\x01",0, maximum_length=39) alt_rank = NULL("") ;
    //   bit_vector_t alt_products;
    // end;
    // type alt_exclusion_products_t =
    // record
    //   decimal("\x01",0, maximum_length=10) tal_assoc_dtl_id ;
    //   decimal("\x01",0, maximum_length=10) tal_assoc_id ;
    //   string("\x01", maximum_length=20) tal_assoc_name ;
    //   decimal("\x01",0, maximum_length=39) tal_assoc_type_cd ;
    //   string("\x01", maximum_length=20) override_tac_name = NULL("") ;
    //   string("\x01", maximum_length=20) override_tar_name = NULL("") ;
    //   bit_vector_t target_product_dtl;
    //   bit_vector_t alt_product_dtl;
    //   string(1) newline = "\n";
    // end;
    // type tal_assoc_xwalk_t =
    // record
    //   decimal("\x01",0, maximum_length=10) tal_assoc_id ;
    //   string("\x01", maximum_length=20) tal_assoc_name ;
    //   string("\x01", maximum_length=30) shared_qual ;
    //   string("\x01", maximum_length=10) override_tac_name = NULL("") ;
    //   string("\x01", maximum_length=10) override_tar_name = NULL("") ;
    //   decimal("\x01",0, maximum_length=39) tal_assoc_type_cd ;
    //   target_udl_products_t[int] target_udl_products;
    //   alt_udl_products_t[int] alt_udl_products;
    //   string(1) newline = "\n";
    // end;
    // type form_data_set_dtl_t =
    // record
    //   decimal("\x01",0, maximum_length=10) formulary_data_set_id ;
    //   string("\x01", maximum_length=20) formulary_name ;
    //   string("\x01", maximum_length=20) formulary_id = NULL("") ;
    //   string("\x01", maximum_length=10) formulary_cd = "" ;
    //   string("\x01", maximum_length=20) carrier = NULL("") ;
    //   string("\x01", maximum_length=20) account = NULL("") ;
    //   string("\x01", maximum_length=20) group = NULL("") ;
    //   string("\x01", maximum_length=20) rxclaim_env_name ;
    //   string("\x01", maximum_length=20) customer_name = NULL("");
    //   date("YYYYMMDD")("\x01") last_exp_dt ;
    //   date("YYYYMMDD")("\x01") run_eff_dt = NULL("") ;
    //   decimal("\x01",0, maximum_length=10) formulary_data_set_dtl_id ;
    //   string("\x01", maximum_length=11) ndc11 ;
    //   string("\x01", maximum_length=2) formulary_tier ;
    //   string("\x01", maximum_length=2) formulary_status ;
    //   string("\x01", maximum_length=1) pa_reqd_ind ;
    //   string("\x01", maximum_length=1) specialty_ind ;
    //   string("\x01", maximum_length=1) step_therapy_ind ;
    //   string("\x01", maximum_length=50) formulary_tier_desc ;
    //   string("\x01", maximum_length=50) formulary_status_desc ;
    //   string("\x01", maximum_length=1) pa_type_cd ;
    //   string("\x01", maximum_length=1) step_therapy_type_cd ;
    //   string("\x01", maximum_length=100) step_therapy_group_name = NULL("") ;
    //   decimal("\x01",0, maximum_length=39) step_therapy_step_number = NULL("") ;
    //   string(1) newline = "\n";
    // end;
    // type form_ovrrd_xwalk_t =
    // record
    //   string("\x01", maximum_length=20) formulary_name ;
    //   string("\x01", maximum_length=10) formulary_cd = "" ;
    //   string("\x01", maximum_length=20)  carrier = NULL("") ;
    //   string("\x01", maximum_length=20)  account = NULL("") ;
    //   string("\x01", maximum_length=20)  group = NULL("") ;
    //   date("YYYYMMDD")("\x01") last_exp_dt ;
    //   string("\x01", maximum_length=11) ndc11 ;
    //   string("\x01", maximum_length=2) formulary_tier ;
    //   string("\x01", maximum_length=2) formulary_status ;
    //   string("\x01", maximum_length=1) pa_reqd_ind ;
    //   string("\x01", maximum_length=1) specialty_ind ;
    //   string("\x01", maximum_length=1) step_therapy_ind ;
    //   string("\x01", maximum_length=50) formulary_tier_desc ;
    //   string("\x01", maximum_length=50) formulary_status_desc ;
    //   string("\x01", maximum_length=1) pa_type_cd ;
    //   string("\x01", maximum_length=1) step_therapy_type_cd ;
    //   string("\x01", maximum_length=100) step_therapy_group_name = NULL("") ;
    //   decimal("\x01",0, maximum_length=39) step_therapy_step_number = NULL("") ;
    //   string(1) newline = "\n";
    // end;
    // type form_ovrrd_ref_file_t =
    // record
    //   string("\x01", maximum_length=20) formulary_name ;
    //   string("\x01", maximum_length=20)  carrier = NULL("") ;
    //   string("\x01", maximum_length=20)  account = NULL("") ;
    //   string("\x01", maximum_length=20)  group = NULL("") ;
    //   string("\x01", maximum_length=20) customer_name = NULL("");
    //   decimal("\x01",0, maximum_length=1) is_future_snap = 0;
    //   string("\x01") data_path;
    //   date("YYYYMMDD")("\x01") run_eff_dt = NULL("") ;
    //   string(1) newline = "\n";
    // end;
    // type output_profile_t =
    // record
    //   decimal("\x01",0, maximum_length=10) output_profile_id ;
    //   string("\x01", maximum_length=20) rxclaim_env_name ;
    //   string("\x01", maximum_length=20) formulary_name ;
    //   string("\x01", maximum_length=20) formulary_id = NULL("") ;
    //   decimal("\x01",0, maximum_length=10) output_profile_form_dtl_id ;
    //   decimal("\x01",0, maximum_length=10) output_profile_job_dtl_id ;
    //   string("\x01", maximum_length=20) output_profile_name ;
    //   string("\x01", maximum_length=20) alias_name = NULL("") ;
    //   decimal("\x01",0, maximum_length=39) alias_priority = NULL ;
    //   string("\x01", maximum_length=20) carrier = NULL("") ;
    //   string("\x01", maximum_length=20) account = NULL("") ;
    //   string("\x01", maximum_length=20) group = NULL("") ;
    //   string("\x01", maximum_length=20) tal_name ;
    //   string("\x01", maximum_length=20) tac_name ;
    //   string("\x01", maximum_length=20) tar_name ;
    //   string("\x01", maximum_length=20) tsd_name ;
    //   decimal("\x01",0, maximum_length=10) job_id ;
    //   string("\x01", maximum_length=20) job_name ;
    //   string("\x01", maximum_length=20) customer_name = NULL("");
    //   decimal("\x01",0, maximum_length=39) run_day = NULL("") ;
    //   string("\x01", maximum_length=20) lob_name = NULL("") ;
    //   string("\x01", maximum_length=4) run_jan1_start_mmdd = NULL("") ;
    //   string("\x01", maximum_length=4) run_jan1_end_mmdd = NULL("") ;
    //   decimal("\x01") future_flg = 0;
    //   string("\x01", maximum_length=30) formulary_pseudonym = NULL("") ;
    //   decimal("\x01",0, maximum_length=10) notes_id = NULL("") ;
    //   string("\x01", maximum_length=60) output_profile_desc ;
    //   decimal("\x01",0, maximum_length=39 ) formulary_option_cd ;
    //   string("\x01", maximum_length=20) layout_name ;
    //   date("YYYYMMDD")("\x01") as_of_dt = NULL("") ;
    //   string("\x01", maximum_length=1) st_tac_ind ;
    //   string(1) newline = "\n";
    // end;
    // type form_product_t =
    // record
    //   string("\x01", maximum_length=10) formulary_cd = "" ;
    //   string("\x01", maximum_length=11) ndc11 ;
    //   string("\x01", maximum_length=2) formulary_tier ;
    //   string("\x01", maximum_length=2) formulary_status ;
    //   string("\x01", maximum_length=1) pa_reqd_ind ;
    //   string("\x01", maximum_length=1) specialty_ind ;
    //   string("\x01", maximum_length=1) step_therapy_ind ;
    //   string("\x01", maximum_length=50) formulary_tier_desc ;
    //   string("\x01", maximum_length=50) formulary_status_desc ;
    //   string("\x01", maximum_length=1) pa_type_cd ;
    //   string("\x01", maximum_length=1) step_therapy_type_cd ;
    //   string("\x01", maximum_length=100) step_therapy_group_name = NULL("") ;
    //   decimal("\x01",0, maximum_length=39) step_therapy_step_number = NULL("") ;
    //   date("YYYYMMDD")("\x01") last_exp_dt ;
    // end;
    // type cag_product_t =
    // record
    //   string("\x01", maximum_length=11) ndc11 ;
    //   string("\x01", maximum_length=14) gpi14 ;
    //   string("\x01", maximum_length=1) status_cd ;
    //   string("\x01", maximum_length=8) eff_dt = NULL("") ;
    //   string("\x01", maximum_length=8) term_dt = NULL("") ;
    //   string("\x01", maximum_length=8) inactive_dt ;
    //   string("\x01", maximum_length=1) msc ;
    //   string("\x01", maximum_length=70) drug_name ;
    //   string("\x01", maximum_length=3) rx_otc ;
    //   string("\x01", maximum_length=1) desi ;
    //   string("\x01", maximum_length=2) roa_cd ;
    //   string("\x01", maximum_length=4) dosage_form_cd ;
    //   decimal("\x01".5, maximum_length=15) prod_strength ;
    //   string("\x01", maximum_length=1) repack_cd ;
    //   string("\x01", maximum_length=30) prod_short_desc ;
    //   string("\x01", maximum_length=60) gpi14_desc ;
    //   string("\x01", maximum_length=60) gpi8_desc ;
    // end;
    // type cag_rlp_t =
    // record
    //   string("\x01", maximum_length=20) carrier = NULL("") ;
    //   string("\x01", maximum_length=20) account = NULL("") ;
    //   string("\x01", maximum_length=20) group = NULL("") ;
    //   date("YYYYMMDD")("\x01") run_eff_dt = NULL("") ;
    //   decimal(1) cag_priority = 0;
    //   cag_product_t[int] prdcts;
    // end;
    // type form_rlp_t =
    // record
    //   string("\x01", maximum_length=20) formulary_name ;
    //   string("\x01", maximum_length=20) carrier = NULL("") ;
    //   string("\x01", maximum_length=20) account = NULL("") ;
    //   string("\x01", maximum_length=20) group = NULL("") ;
    //   string("\x01", maximum_length=20) customer_name = NULL("");
    //   date("YYYYMMDD")("\x01") run_eff_dt = NULL("") ;
    //   decimal(1) cag_priority = 0;
    //   form_product_t[int] prdcts;
    // end;
    // type tsd_dtl_t =
    // record
    //   decimal("\x01",0, maximum_length=10) tsd_dtl_id ;
    //   decimal("\x01",0, maximum_length=10) tsd_id ;
    //   string("\x01", maximum_length=20) tsd_name ;
    //   string("\x01", maximum_length=30) tsd_cd ;
    //   string("\x01", maximum_length=2) formulary_tier ;
    //   string("\x01", maximum_length=2) formulary_status ;
    //   decimal("\x01",0, maximum_length=39) priority ;
    //   date("YYYYMMDD")("\x01") eff_dt ;
    //   date("YYYYMMDD")("\x01") term_dt ;
    //   string(1) newline = "\n";
    // end;
    // type tsd_xwalk_t =
    // record
    //   decimal("\x01",0, maximum_length=10) tsd_id ;
    //   string("\x01", maximum_length=30) tsd_cd ;
    //   bit_vector_t products;
    //   string(1) newline = "\n";
    // end;
    // type alt_constituent_prdct_t =
    // record
    //   bit_vector_t alt_prdcts;
    //   string(2) constituent_group = NULL("");
    //   string(1) constituent_reqd = NULL("");
    //   string(20) udl_nm = NULL("");
    // end;
    // type tal_container_t =
    // record
    //   decimal("\x01",0, maximum_length=10) tal_id ;
    //   string("\x01", maximum_length=20) tal_name ;
    //   string("\x01", maximum_length=20) tal_assoc_name = NULL("") ;
    //   string("\x01", maximum_length=20) tar_udl_nm = NULL("") ;
    //   string("\x01", maximum_length=60) tal_desc ;
    //   decimal("\x01", 6, maximum_length=39) priority ;
    //   decimal("\x01",0, maximum_length=39) tal_assoc_type_cd ;
    //   bit_vector_t[int] target_prdcts;
    //   alt_constituent_prdct_t[int] alt_constituent_prdcts;
    //   string("\x01", maximum_length=30) shared_qual ;
    //   string("\x01", maximum_length=20) override_tac_name = NULL("") ;
    //   string("\x01", maximum_length=20) override_tar_name = NULL("") ;
    //   string(2)[int] constituent_grp_vec;
    //   string(1) newline = "\n";
    // end;
    // type master_cag_mapping_t =
    // record
    //   string("\x01") carrier = NULL("") ;
    //   string("\x01") account = NULL("") ;
    //   string("\x01") group = NULL("") ;
    //   string("\x01") future_flg = 'C';
    //   string("\x01") cag_override_data_path= NULL("");
    //   decimal("\x01")[int] qual_output_profile_ids ;
    //   record
    //     string("\x01") qual_output_profile_name ;
    //     string("\x01") rxclaim_env_name;
    //     decimal("\x01") [int] job_ids;
    //     string("\x01") [int] alias_names;
    //     string("\x01") [int] job_names;
    //     string("\x01") [int] formulary_names ;
    //     string("\x01") [int] form_override_data_paths;
    //     string("\x01", maximum_length=30)[int] formulary_pseudonyms;
    //     string("\x01", maximum_length=20) tal_name ;
    //     string("\x01") tac_name ;
    //     string("\x01") tar_name ;
    //     string("\x01") tsd_name ;
    //     string("\x01", maximum_length=1) st_tac_ind ;
    //   end[int] op_dtls;
    //   record
    //     decimal("\x01") non_qual_op_id;
    //     string("\x01") rxclaim_env_name;
    //     decimal("\x01") [int] job_ids;
    //     string("\x01")[int] formulary_names;
    //     string("\x01", maximum_length=30)[int] formulary_pseudonyms;
    //     string("\x01") [int] alias_names;
    //     string("\x01") [int] job_names;
    //   end[int] non_qual_output_profile_ids ;
    //   string("\x01") [int] err_msgs;
    //   date("YYYYMMDD")("\x01") as_of_dt = NULL("");
    //   string(1) newline = "\n";
    // end;
    // type tar_dtl_t =
    // record
    //   decimal("\x01",0, maximum_length=10) tar_id ;
    //   decimal("\x01",0, maximum_length=10) tar_dtl_id ;
    //   string("\x01", maximum_length=20) tar_name ;
    //   string("\x01", maximum_length=1) sort_ind ;
    //   string("\x01", maximum_length=1) filter_ind ;
    //   decimal("\x01",0, maximum_length=39) priority ;
    //   decimal("\x01",0, maximum_length=39) tar_dtl_type_cd ;
    //   decimal("\x01",0, maximum_length=10) tar_roa_df_set_id = NULL("") ;
    //   string("\x01", maximum_length=4000) target_rule = NULL("") ;
    //   string("\x01", maximum_length=4000) alt_rule = NULL("") ;
    //   string("\x01", maximum_length=15) rebate_elig_cd = NULL("") ;
    //   date("YYYYMMDD")("\x01") eff_dt ;
    //   date("YYYYMMDD")("\x01") term_dt ;
    //   string(1) newline = "\n";
    // end;
    // type tar_roa_df_set_dtl_t =
    // record
    //   decimal("\x01",0, maximum_length=10) tar_roa_df_set_dtl_id ;
    //   decimal("\x01",0, maximum_length=10) tar_roa_df_set_id ;
    //   string("\x01", maximum_length=2) target_roa_cd = NULL("") ;
    //   string("\x01", maximum_length=4) target_dosage_form_cd = NULL("") ;
    //   string("\x01", maximum_length=2) alt_roa_cd = NULL("") ;
    //   string("\x01", maximum_length=4) alt_dosage_form_cd = NULL("") ;
    //   decimal("\x01",0, maximum_length=39) priority ;
    //   string(1) newline = "\n";
    // end;
    // type tar_rule_xwalk_t =
    // record
    //   decimal("\x01",0, maximum_length=10) tar_id ;
    //   decimal("\x01",0, maximum_length=10) tar_dtl_id ;
    //   string("\x01", maximum_length=20) tar_name ;
    //   decimal("\x01",0, maximum_length=10) tar_roa_df_set_id = NULL("");
    //   decimal("\x01",0, maximum_length=39) tar_dtl_type_cd ;
    //   decimal("\x01",0, maximum_length=39) priority ;
    //   string("\x01", maximum_length=1) sort_ind ;
    //   string("\x01", maximum_length=1) filter_ind ;
    //   string("\x01", maximum_length=15) rebate_elig_cd = NULL("") ;
    //   date("YYYYMMDD")("\x01") eff_dt ;
    //   date("YYYYMMDD")("\x01") term_dt ;
    //   record
    //    string(int) qualifier_cd;
    //    string(int) operator;
    //    string(int) compare_value;
    //    string(1) conjunction_cd;
    //   end[int] target_rule_def;
    //   record
    //     string(int) qualifier_cd;
    //     string(int) operator;
    //     string(int) compare_value;
    //     string(1) conjunction_cd;
    //   end[int] alt_rule_def;
    //   string(1) newline = "\n";
    // end;
    // type alt_run_alt_dtl_load_t =
    // record
    //   decimal("\x01",0, maximum_length=16 ) alt_run_alt_dtl_id = -1 ;
    //   decimal("\x01",0, maximum_length=16 ) alt_run_id = -1;
    //   decimal("\x01",0, maximum_length=16 ) alt_run_target_dtl_id = -1 ;
    //   string("\x01", maximum_length=20) formulary_name ;
    //   string("\x01", maximum_length=11) target_ndc ;
    //   string("\x01", maximum_length=11) alt_ndc ;
    //   string("\x01", maximum_length=2) alt_formulary_tier ;
    //   string("\x01", maximum_length=1) alt_multi_src_cd ;
    //   string("\x01", maximum_length=2) alt_roa_cd ;
    //   string("\x01", maximum_length=4) alt_dosage_form_cd ;
    //   decimal("\x01",0, maximum_length=39 ) rank ;
    //   string("\x01", maximum_length=1) alt_step_therapy_ind ;
    //   string("\x01", maximum_length=1) alt_pa_reqd_ind ;
    //   string("\x01", maximum_length=2) alt_formulary_status ;
    //   string("\x01", maximum_length=1) alt_specialty_ind ;
    //   string("\x01", maximum_length=14) alt_gpi14 ;
    //   string("\x01", maximum_length=70) alt_prod_name_ext ;
    //   string("\x01", maximum_length=30) alt_prod_short_desc ;
    //   string("\x01", maximum_length=30) alt_prod_short_desc_grp;
    //   string("\x01", maximum_length=60) alt_gpi14_desc ;
    //   string("\x01", maximum_length=60) alt_gpi8_desc ;
    //   string("\x01", maximum_length=50) alt_formulary_tier_desc ;
    //   string("\x01", maximum_length=50) alt_formulary_status_desc ;
    //   string("\x01", maximum_length=1) alt_pa_type_cd ;
    //   string("\x01", maximum_length=1) alt_step_therapy_type_cd ;
    //   string("\x01", maximum_length=1000) alt_step_therapy_group_name = NULL("") ;
    //   string("\x01", maximum_length=100) alt_step_therapy_step_number = NULL("") ;
    //   datetime("YYYY-MM-DD HH24:MI:SS")("\x01") rec_crt_ts ;
    //   string("\x01", maximum_length=30) rec_crt_user_id ;
    //   string("\x01", maximum_length=15) rebate_elig_cd = NULL("") ;
    //   string("\x01", maximum_length=15) tad_eligible_cd= NULL("");
    //   decimal("\x01".3, maximum_length=7) alt_qty_adj = NULL("") ;
    //   string("\x01", maximum_length=40) tal_assoc_name = NULL("") ;
    //   string("\x01", maximum_length=40) tala = NULL("") ;
    //   string("\x01", maximum_length=20) alt_udl = NULL("") ;
    //   decimal("\x01".6, maximum_length=39) tal_assoc_rank ;
    //   string("\x01", maximum_length=2) constituent_group = NULL("") ;
    //   string("\x01", maximum_length=1) constituent_reqd = NULL("") ;
    //   string(1) newline = "\n";
    // end;
    // type alt_run_target_dtl_load_t =
    // record
    //   decimal("\x01",0, maximum_length=16 ) alt_run_target_dtl_id = -1 ;
    //   decimal("\x01",0, maximum_length=16 ) alt_run_id  = -1 ;
    //   string("\x01", maximum_length=40) tal_assoc_name = NULL("") ;
    //   string("\x01", maximum_length=40) tala = NULL("") ;
    //   string("\x01", maximum_length=20) tar_udl = NULL("") ;
    //   string("\x01", maximum_length=20) formulary_name ;
    //   string("\x01", maximum_length=11) target_ndc ;
    //   string("\x01", maximum_length=2) target_formulary_tier ;
    //   string("\x01", maximum_length=2) target_formulary_status ;
    //   string("\x01", maximum_length=1) target_pa_reqd_ind ;
    //   string("\x01", maximum_length=1) target_step_therapy_ind ;
    //   string("\x01", maximum_length=1) target_specialty_ind ;
    //   string("\x01", maximum_length=1) target_multi_src_cd ;
    //   string("\x01", maximum_length=2) target_roa_cd ;
    //   string("\x01", maximum_length=4) target_dosage_form_cd ;
    //   string("\x01", maximum_length=14) target_gpi14 ;
    //   string("\x01", maximum_length=70) target_prod_name_ext ;
    //   string("\x01", maximum_length=30) target_prod_short_desc ;
    //   string("\x01", maximum_length=60) target_gpi14_desc ;
    //   string("\x01", maximum_length=60) target_gpi8_desc ;
    //   string("\x01", maximum_length=50) target_formulary_tier_desc ;
    //   string("\x01", maximum_length=50) target_formulary_status_desc ;
    //   string("\x01", maximum_length=1) target_pa_type_cd ;
    //   string("\x01", maximum_length=1) target_step_therapy_type_cd ;
    //   string("\x01", maximum_length=1000) target_step_therapy_group_name = NULL("") ;
    //   decimal("\x01",0, maximum_length=39) target_step_therapy_step_num = NULL("") ;
    //   string("\x01", maximum_length=10) formulary_cd = NULL("") ;
    //   datetime("YYYY-MM-DD HH24:MI:SS")("\x01") rec_crt_ts ;
    //   string("\x01", maximum_length=30) rec_crt_user_id ;
    //   date("YYYYMMDD")("\x01") last_exp_dt ;
    //   string(1) newline = "\n";
    // end;
    // type alt_run_load_t =
    // record
    //   decimal("\x01",0, maximum_length=16) alt_run_id = -1 ;
    //   decimal("\x01",0, maximum_length=10) output_profile_id ;
    //   datetime("YYYY-MM-DD HH24:MI:SS")("\x01") run_start_ts = NULL("") ;
    //   datetime("YYYY-MM-DD HH24:MI:SS")("\x01") run_complete_ts = NULL("") ;
    //   decimal("\x01",0, maximum_length=39) alt_run_status_cd ;
    //   string("\x01", maximum_length=1) published_ind ;
    //   datetime("YYYY-MM-DD HH24:MI:SS")("\x01") rec_crt_ts ;
    //   string("\x01", maximum_length=30) rec_crt_user_id ;
    //   datetime("YYYY-MM-DD HH24:MI:SS")("\x01") rec_last_upd_ts ;
    //   string("\x01", maximum_length=30) rec_last_upd_user_id ;
    //   date("YYYYMMDD")("\x01") run_eff_dt ;
    //   date("YYYYMMDD")("\x01") as_of_dt = NULL("") ;
    //   string(1) newline = "\n";
    // end;
    // type alt_run_load_adhoc_t =
    // record
    //   decimal("\x01",0, maximum_length=16) alt_run_id = -1 ;
    //   decimal("\x01",0, maximum_length=10) output_profile_id ;
    //   datetime("YYYY-MM-DD HH24:MI:SS")("\x01") run_start_ts = NULL("") ;
    //   datetime("YYYY-MM-DD HH24:MI:SS")("\x01") run_complete_ts = NULL("") ;
    //   decimal("\x01",0, maximum_length=39) alt_run_status_cd ;
    //   string("\x01", maximum_length=1) published_ind ;
    //   datetime("YYYY-MM-DD HH24:MI:SS")("\x01") rec_crt_ts ;
    //   string("\x01", maximum_length=30) rec_crt_user_id ;
    //   datetime("YYYY-MM-DD HH24:MI:SS")("\x01") rec_last_upd_ts ;
    //   string("\x01", maximum_length=30) rec_last_upd_user_id ;
    //   date("YYYYMMDD")("\x01") run_eff_dt ;
    //   string(1) newline = "\n";
    // end;
    // type alt_run_t =
    // record
    //   decimal("|") alt_run_id ;
    //   decimal("|") output_profile_id ;
    //   datetime("YYYY-MM-DD HH24:MI:SS")("|") run_start_ts = NULL("") ;
    //   datetime("YYYY-MM-DD HH24:MI:SS")("|") run_complete_ts = NULL("") ;
    //   decimal("|") alt_run_status_cd ;
    //   string("|") published_ind ;
    //   datetime("YYYY-MM-DD HH24:MI:SS")("|") rec_crt_ts ;
    //   string("|") rec_crt_user_id ;
    //   datetime("YYYY-MM-DD HH24:MI:SS")("|") rec_last_upd_ts ;
    //   string("|") rec_last_upd_user_id ;
    //   date("YYYYMMDD")("\n") run_eff_dt ;
    // end;
    // type alt_run_alt_proxy_dtl_load_t =
    // record
    //   decimal("\x01",0, maximum_length=16 ) alt_run_id = -1;
    //   decimal("\x01",0, maximum_length=16 ) alt_run_alt_dtl_id = -1 ;
    //   decimal("\x01",0, maximum_length=16 ) alt_run_target_dtl_id = -1 ;
    //   string("\x01", maximum_length=20) formulary_name ;
    //   string("\x01", maximum_length=11) target_ndc ;
    //   string("\x01", maximum_length=40) tal_assoc_name ;
    //   string("\x01", maximum_length=11) alt_proxy_ndc ;
    //   decimal("\x01",0, maximum_length=39 ) rank ;
    //   datetime("YYYY-MM-DD HH24:MI:SS")("\x01") rec_crt_ts ;
    //   string("\x01", maximum_length=30) rec_crt_user_id ;
    //   string(1) newline = "\n";
    // end;
    // type file_load_cntl_load_t =
    // record
    //   string("\x01", maximum_length=50) user_id ;
    //   string("\x01", maximum_length=1) file_load_type_cd ;
    //   decimal("\x01",0, maximum_length=39) component_type_cd = NULL("") ;
    //   decimal("\x01",0, maximum_length=39) file_load_status_cd ;
    //   datetime("YYYY-MM-DD HH24:MI:SS")("\x01") file_load_start_ts = NULL("") ;
    //   datetime("YYYY-MM-DD HH24:MI:SS")("\x01") rec_crt_ts ;
    //   string("\x01", maximum_length=30) rec_crt_user_id ;
    //   string("\x01", maximum_length=1) published_ind ;
    //   decimal("\x01",0, maximum_length=16) alt_run_id = NULL("") ;
    //   string(1) newline = "\n";
    // end;
    // type file_load_ctl_dtl_t =
    // record
    //   decimal("\x01",0, maximum_length=16) file_load_cntl_id ;
    //   string("\x01", maximum_length=4000) component_ids = NULL("") ;
    //   date("YYYYMMDD")("\x01") as_of_date ;
    //   string("\x01", maximum_length=20) rxclaim_env_name = NULL("") ;
    //   string("\x01", maximum_length=20) carrier = NULL("") ;
    //   string("\x01", maximum_length=20) account = NULL("") ;
    //   string("\x01", maximum_length=20) group = NULL("") ;
    //   decimal("\x01",0, maximum_length=39) component_type_cd = NULL("") ;
    //   string("\x01", maximum_length=512) file_name_w = NULL("") ;
    //   string("\x01", maximum_length=1) published_ind ;
    //   decimal("\x01",0, maximum_length=16) alt_run_id = NULL("") ;
    //   string("\x01", maximum_length=512) report_file_name = NULL("") ;
    //   string(1) newline = "\n";
    // end;
    // type file_load_err_dtl_load_t =
    // record
    //   decimal(",",0, maximum_length=16) file_load_cntl_id ;
    //   string(",", maximum_length=1024) err_desc ;
    //   datetime("YYYY-MM-DD HH24:MI:SS")(",") rec_crt_ts ;
    //   string(",", maximum_length=30) rec_crt_user_id ;
    //   datetime("YYYY-MM-DD HH24:MI:SS")(",") rec_last_upd_ts ;
    //   string("\n", maximum_length=30) rec_last_upd_user_id ;
    // end;
    // type tal_assoc_prdcts_t =
    // record
    //   decimal("\x01",0, maximum_length=10) tal_assoc_dtl_id ;
    //   decimal("\x01",0, maximum_length=10) tal_assoc_id ;
    //   string("\x01", maximum_length=20) tal_assoc_name ;
    //   decimal("\x01",0, maximum_length=39) tal_assoc_type_cd ;
    //   bit_vector_t[int] target_prdcts;
    //   bit_vector_t[int] alt_prdcts;
    //   string("\x01", maximum_length=30) shared_qual ;
    //   string("\x01", maximum_length=20) override_tac_name = NULL("") ;
    //   string("\x01", maximum_length=20) override_tar_name = NULL("") ;
    //   string(1) newline = "\n";
    // end;
    // type part_exp_master_file_t =
    // record
    //   decimal("\x01",0, maximum_length=16) file_load_cntl_id ;
    //   string("\x01", maximum_length=1024) component_ids ;
    //   date("YYYYMMDD")("\x01") as_of_date ;
    //   string("\x01", maximum_length=20) rxclaim_env_name = "" ;
    //   string("\x01", maximum_length=50) cag_in = NULL("") ;
    //   string("\x01", maximum_length=50) cag_out = NULL("") ;
    //   decimal("\x01",0, maximum_length=39, sign_reserved) component_type_cd = NULL("") ;
    //   utf8 string("\x01", maximum_length=512) file_name_w = NULL("") ;
    //   utf8 string("\x01", maximum_length=1) published_ind ;
    //   string(1) newline = "\n";
    // end;
    // type alt_rank_t =
    // record
    //   int alt_rank;
    //   int[int] alt_prdcts;
    //   string(2) constituent_group = NULL("");
    //   string(1) constituent_reqd = NULL("");
    //   string(20) udl_nm = NULL("");
    // end;
    // type rebate_elig_contents_t =
    // record
    //   string("\x01") rebate_elig_cd;
    //   int[int] rebate_elig_prdcts;
    // end;
    // type target_prdct_t =
    // record
    //   int target_dl_bit;
    //   decimal('\x01') bucket_index = 0;
    //   decimal('\x01') udl_index = 0;
    //   decimal('\x01') tac_index = 0;
    //   alt_rank_t[int] alt_prdcts_rank;
    //   string("\x01")has_alt;
    //   decimal(1) ST_flag = 0;
    // end;
    // type tal_assoc_adhoc_t =
    // record
    //   decimal("\x01",0, maximum_length=10) tal_assoc_dtl_id ;
    //   decimal("\x01",0, maximum_length=10) tal_assoc_id ;
    //   string("\x01", maximum_length=20) tal_assoc_name ;
    //   string("\x01", maximum_length=2000) clinical_indn_desc = NULL("");
    //   string("\x01", maximum_length=60) tal_assoc_desc ;
    //   decimal("\x01",0, maximum_length=39) tal_assoc_type_cd ;
    //   record
    //     string("\x01", maximum_length=20) udl_nm;
    //     string("\x01", maximum_length=60) udl_desc;
    //     bit_vector_t products;
    //   end[int] target_udl_info ;
    //   record
    //      string("\x01", maximum_length=20) udl_nm;
    //      string("\x01", maximum_length=60) udl_desc;
    //      bit_vector_t products;
    //      string(2) constituent_group = NULL("");
    //      string(1) constituent_reqd = NULL("");
    //      decimal("\x01",0, maximum_length=39) constituent_rank = NULL("");
    //   end[int] alt_udl_info;
    //   string("\x01", maximum_length=30) shared_qual ;
    //   string("\x01", maximum_length=20) override_tac_name = NULL("") ;
    //   string("\x01", maximum_length=20) override_tar_name = NULL("") ;
    //   string(1) newline = "\n";
    // end;
    // type tal_container_adhoc_t =
    // record
    //   decimal("\x01",0, maximum_length=10) tal_id ;
    //   string("\x01", maximum_length=20) tal_name ;
    //   string("\x01", maximum_length=20) tal_assoc_name = NULL("") ;
    //   string("\x01", maximum_length=2000) clinical_indn_desc = NULL("");
    //   string("\x01", maximum_length=60) tal_desc ;
    //   string("\x01", maximum_length=60) tal_assoc_desc ;
    //   decimal("\x01", 6, maximum_length=39) priority ;
    //   decimal("\x01",0, maximum_length=39) tal_assoc_type_cd ;
    //   record
    //       string("\x01", maximum_length=20) udl_nm;
    //       string("\x01", maximum_length=60) udl_desc;
    //       bit_vector_t products;
    //   end[int] target_prdcts;
    //   record
    //       string("\x01", maximum_length=20) udl_nm;
    //       string("\x01", maximum_length=60) udl_desc;
    //       bit_vector_t products;
    //       string(2) constituent_group = NULL("");
    //       string(1) constituent_reqd = NULL("");
    //       decimal("\x01",0, maximum_length=39) constituent_rank = NULL("");
    //   end[int] alt_prdcts;
    //   string("\x01", maximum_length=30) shared_qual ;
    //   string("\x01", maximum_length=20) override_tac_name = NULL("") ;
    //   string("\x01", maximum_length=20) override_tar_name = NULL("") ;
    //   string(1) newline = "\n";
    // end;
    // type tal_assoc_adhoc_enrich_t =
    // record
    //   decimal("\x01",0, maximum_length=10) tal_assoc_dtl_id ;
    //   decimal("\x01",0, maximum_length=10) tal_assoc_id ;
    //   string("\x01", maximum_length=20) tal_assoc_name ;
    //   string("\x01", maximum_length=2000) clinical_indn_desc = NULL("");
    //   string("\x01", maximum_length=60) tal_assoc_desc ;
    //   decimal("\x01",0, maximum_length=39) tal_assoc_type_cd ;
    //   string("\x01", maximum_length=20) udl_id = NULL("") ;
    //   string("\x01", maximum_length=60) udl_desc;
    //   string("\x01", maximum_length=20) Target_Alternative;
    //   bit_vector_t products;
    //   decimal("\x01",0, maximum_length=39) alt_rank = NULL("") ;
    //   string("\x01", maximum_length=30) shared_qual ;
    //   string("\x01", maximum_length=20) override_tac_name = NULL("") ;
    //   string("\x01", maximum_length=20) override_tar_name = NULL("") ;
    //   string(2) constituent_group = NULL("");
    //   string(1) constituent_reqd = NULL("");
    //   decimal("\x01",0, maximum_length=39) constituent_rank = NULL("");
    //   string(1) newline = "\n";
    // end;
    // type tal_container_adhoc_enrich_t =
    // record
    //   string("\x01", maximum_length=20) tal_id ;
    //   string("\x01", maximum_length=20) tal_assoc_id = NULL("") ;
    //   string("\x01", maximum_length=2000) clinical_indn_desc = NULL("");
    //   string("\x01", maximum_length=60) tal_desc ;
    //   string("\x01", maximum_length=60) tal_assoc_desc ;
    //   decimal("\x01",0, maximum_length=39) tal_assoc_type_cd ;
    //   decimal("\x01", 6, maximum_length=39) tal_assoc_rank = NULL("") ;
    //   string("\x01", maximum_length=20) udl_id = NULL("") ;
    //   string("\x01", maximum_length=60) udl_desc;
    //   string("\x01", maximum_length=20) Target_Alternative;
    //   bit_vector_t products;
    //   decimal("\x01",0, maximum_length=39) alt_rank = NULL("") ;
    //   string("\x01", maximum_length=30) shared_qual ;
    //   string("\x01", maximum_length=20) override_tac_name = NULL("") ;
    //   string("\x01", maximum_length=20) override_tar_name = NULL("") ;
    //   string(2) constituent_group = NULL("");
    //   string(1) constituent_reqd = NULL("");
    //   decimal("\x01",0, maximum_length=39) constituent_rank = NULL("");
    //   string(1) newline = "\n";
    // end;
    // type error_info_V2_16 = record
    //   utf8 string(big endian integer(4)) component;
    //   big endian integer(4) port_index;
    //   utf8 string(big endian integer(4)) parameter;
    //   utf8 string(big endian integer(4)) message;
    //   record
    //     utf8 string(big endian integer(4)) code;
    //     big endian integer(4) parent_index;
    //     record
    //       utf8 string(big endian integer(4)) name;
    //       utf8 string(big endian integer(4)) value;
    //     end [big endian integer(4)] attributes;
    //   end [big endian integer(4)] details;
    // end;
    // type error_info_t = error_info_V2_16;
    // type alt_run_target_dtl_ref_t =
    // record
    //   decimal("\x01",0, maximum_length=16 ) alt_run_target_dtl_id ;
    //   string("\x01", maximum_length=40) tal_assoc_name ;
    //   string("\x01", maximum_length=20) formulary_name ;
    //   string("\x01", maximum_length=11) target_ndc ;
    // end;
    // type alt_run_alt_dtl_ref_t =
    // record
    //   decimal("\x01",0, maximum_length=16 ) alt_run_alt_dtl_id ;
    //   decimal("\x01",0, maximum_length=16 ) alt_run_target_dtl_id ;
    //   string("\x01", maximum_length=20) formulary_name ;
    //   string("\x01", maximum_length=40) tal_assoc_name ;
    //   string("\x01", maximum_length=11) target_ndc ;
    //   string("\x01", maximum_length=11) alt_ndc ;
    // end;
    // type master_cag_mapping_adhoc_t =
    // record
    //   string("\x01") carrier = NULL("") ;
    //   string("\x01") account = NULL("") ;
    //   string("\x01") group = NULL("") ;
    //   string("\x01") cag_override_data_path= NULL("");
    //   decimal("\x01")[int] output_profile_id ;
    //   record
    //     string("\x01") output_profile_name ;
    //     string("\x01") [int] alias_names;
    //     string("\x01") [int] job_names;
    //     string("\x01")[int] formulary_names ;
    //     string("\x01") [int] form_override_data_paths;
    //     string("\x01")[int] formulary_pseudonyms ;
    //     string("\x01") tal_name ;
    //     string("\x01") tac_name ;
    //     string("\x01") tar_name ;
    //     string("\x01") tsd_name ;
    //     string("\x01", maximum_length=1) st_tac_ind ;
    //   end[int] op_dtls;
    //   string("\x01")[int] err_msgs ;
    //   string(1) newline = "\n";
    // end;
    // type tal_expansion_test_harness =
    // record
    //   string('\x01') tal_id;
    //   string('\x01') tal_assoc_id;
    //   string('\x01') tal_assoc_desc;
    //   string('\x01') tal_assoc_type_cd;
    //   string('\x01') override_tac_name;
    //   string('\x01') override_tar_name;
    //   string('\x01') shared_qual;
    //   string('\x01') tal_assoc_rank;
    //   string('\x01') udl_id;
    //   string('\x01') udl_desc;
    //   string('\x01') alt_rank;
    //   string('\x01') target_alternative;
    //   string('\x01') ndc11;
    //   string('\x01') gpi14;
    //   string('\x01') msc;
    //   string('\x01') drug_name;
    //   string('\x01') prod_short_desc;
    //   string('\x01') gpi14_desc;
    //   string('\x01') prod_strength;
    //   string('\x01') roa_cd;
    //   string('\x01') dosage_form_cd;
    //   string('\x01') rx_otc;
    //   string('\x01') repack_cd;
    //   string('\x01') status_cd;
    //   string('\x01') inactive_dt;
    //   string(1) newline = '\n';
    // end;
    // type udl_expansion_test_harness =
    // record
    //   string('\x01')udl_nm ;
    //   string('\x01')ndc11 ;
    //   string('\x01')gpi14 ;
    //   string('\x01')msc ;
    //   string('\x01')drug_name ;
    //   string('\x01')prod_short_desc ;
    //   string('\x01')gpi14_desc ;
    //   string('\x01')prod_strength;
    //   string('\x01')roa_cd ;
    //   string('\x01')dosage_form_cd ;
    //   string('\x01')rx_otc ;
    //   string('\x01')repack_cd;
    //   string('\x01')status_cd ;
    //   string('\x01')inactive_dt ;
    //   string(1)newline = '\n';
    // end;
    // type alt_run_job_details_load_t =
    // record
    //   decimal("\x01",0, maximum_length=16) alt_run_id ;
    //   decimal("\x01",0, maximum_length=16) job_run_id ;
    //   datetime("YYYY-MM-DD HH24:MI:SS")("\x01") rec_crt_ts ;
    //   string("\x01", maximum_length=30) rec_crt_user_id ;
    //   string(1) newline = "\n";
    // end;
    // type alias_dtl_t =
    // record
    //   decimal("\x01",0, maximum_length=10) alias_dtl_id ;
    //   decimal("\x01",0, maximum_length=10) alias_id ;
    //   string("\x01", maximum_length=20) alias_name ;
    //   decimal("\x01",0, maximum_length=39) qual_priority = 99;
    //   string("\x01", maximum_length=10) qual_id_type_cd = NULL("") ;
    //   string("\x01", maximum_length=14) qual_id_value = NULL("") ;
    //   string("\x01", maximum_length=70) search_txt ;
    //   string("\x01", maximum_length=70) replace_txt = NULL("") ;
    //   decimal("\x01",0, maximum_length=39) rank ;
    //   date("YYYYMMDD")("\x01") eff_dt ;
    //   date("YYYYMMDD")("\x01") term_dt ;
    //   string(1) newline = "\n";
    // end;
    // type alias_info_t =  record
    //         decimal("\x01",0, maximum_length=39) qual_priority = 99;
    //         string("\x01", maximum_length=14) qual_id_value = NULL("") ;
    //         string("\x01", maximum_length=70) search_txt ;
    //         string("\x01", maximum_length=70) replace_txt  ;
    //         date("YYYYMMDD")("\x01") eff_dt;
    //         date("YYYYMMDD")("\x01") term_dt;
    // end;
    // type alias_xwalk_t =
    // record
    //   decimal("\x01",0, maximum_length=10) alias_id ;
    //   string("\x01", maximum_length=20) alias_name ;
    //   alias_info_t[int] alias_info;
    // end;
    // type target_alt_rank_t =
    // record
    //     int target_dl_bit;
    //     decimal('\x01') bucket_index = 0;
    //     decimal('\x01') udl_index = 0;
    //     string(20) tar_udl_nm =NULL("");
    //     decimal('\x01') tac_index = 0;
    //     record
    //       int alt_rank;
    //       int[int] alt_prdcts;
    //       string(2) constituent_group = NULL("");
    //       string(1) constituent_reqd = NULL("");
    //       string(20) udl_nm =NULL("");
    //     end[int] alt_prdcts_rank;
    //     string("\x01")has_alt;
    //     decimal(1) ST_flag =0;
    // end;
    // type alias_content_xwalk_t =
    // record
    //   string("\x01", maximum_length=13) alias_set_name;
    //   string("\x01", maximum_length=11)  ndc11 ;
    //   string("\x01") alias_label_nm ;
    //   string(1) newline = "\n";
    // end;
    // type gpi_rank_ratio_t =
    // record
    //   string("\x01", maximum_length=14) gpi14 ;
    //   decimal("\x01",0, maximum_length=39) rank ;
    //   decimal("\x01".3, maximum_length=6) ratio = NULL("") ;
    //   date("YYYYMMDD")("\x01") run_eff_dt = NULL("") ;
    //  end;
    // type tad_t =
    // record
    //   decimal("\x01",0, maximum_length=10) tad_id ;
    //   string("\x01", maximum_length=14) target_gpi14 ;
    //   string("\x01", maximum_length=12) alt_grouping_gpi12 ;
    //   string("\x01", maximum_length=14) alt_selection_id ;
    //   decimal("\x01",0, maximum_length=39) rank ;
    //   decimal("\x01".3, maximum_length=7) qty_adjust_factor = NULL("") ;
    //   string(1) newline = "\n";
    // end;
    // type tad_xwalk_t =
    // record
    //   string("\x01", maximum_length=14) target_gpi14 ;
    //   string("\x01", maximum_length=6) alt_selection_cd;
    //   string("\x01", maximum_length=14)[int] alt_selection_ids ;
    //   record
    //     decimal("\x01",0, maximum_length=39) rank ;
    //     decimal("\x01".3, maximum_length=7) qty_adjust_factor = NULL("") ;
    //   end[int] tad_alt_dtls ;
    //   string(1) newline = "\n";
    // end;
    // type step_grp_xwalk_t =
    // record
    //   string("\x01", maximum_length=11) ndc11 ;
    //   decimal("\x01",0, maximum_length=39) step_therapy_step_number = NULL("") ;
    //   string("\x01", maximum_length=100)[int] step_therapy_group_names  ;
    // end;
    // type step_grp_num_xwalk_t =
    // record
    //   decimal("\x01",0, maximum_length=10) tac_id ;
    //   string("\x01", maximum_length=20) tac_name ;
    //   decimal("\x01",0, maximum_length=39) priority ;
    //   decimal("\x01") _target_st_grp_num = allocate();
    //   decimal("\x01")[int] _alt_st_grp_nums = allocate();
    // end;
    // type tal_assoc_clinical_indn_t =
    // record
    //  decimal("\x01",0, maximum_length=10) tal_assoc_id ;
    //  string("\x01", maximum_length=40) tal_assoc_name ;
    //  decimal("\x01",0, maximum_length=10) clinical_indn_id = NULL("");
    //  string("\x01", maximum_length=20) clinical_indn_name ;
    //  string("\x01", maximum_length=200) clinical_indn_desc = NULL("");
    //  decimal("\x01",0, maximum_length=39) rank = NULL("");
    // end;
    // type alt_run_clinical_indn_dtl_load_t =
    // record
    //   decimal("\x01",0, maximum_length=16) alt_run_id = -1 ;
    //   decimal("\x01",0, maximum_length=16) alt_run_target_dtl_id = -1 ;
    //   string("\x01", maximum_length=20) formulary_name ;
    //   string("\x01", maximum_length=11) target_ndc ;
    //   string("\x01", maximum_length=40) tal_assoc_name ;
    //   decimal("\x01",0, maximum_length=39) rank ;
    //   datetime("YYYY-MM-DD HH24:MI:SS")("\x01") rec_crt_ts ;
    //   string("\x01", maximum_length=30) rec_crt_user_id ;
    //   string("\x01", maximum_length=200) clinical_indn_desc = NULL("") ;
    //   string(1) newline = "\n";
    // end;
    // type form_target_reject_t =
    // record
    // string(",") error_msg;
    // decimal(",") tal_id ;
    // string(",") tal_name ;
    // string(",") tal_assoc_name = NULL("") ;
    // string(",") ndc11 = NULL("");
    // string(",") gpi14 = NULL("");
    // string("\n") prod_short_desc = NULL("");
    // end;
    // type form_alt_reject_t =
    // record
    // string(",") error_msg;
    // decimal(",") tal_id ;
    // string(",") tal_name ;
    // string(",") tal_assoc_name = NULL("") ;
    // string(",") target_ndc11 = NULL("");
    // string(",") alt_ndc11 = NULL("");
    // string(",") gpi14 = NULL("");
    // string("\n") prod_short_desc = NULL("");
    // end;
    // type tal_assoc_enriched_data_t =
    // record
    //   string("\x01", maximum_length=20) tal_assoc_name ;
    //   decimal("\x01",0, maximum_length=39) tal_assoc_type_cd ;
    //   string("\x01", maximum_length=20) Target_Alternative;
    //   string("\x01", maximum_length=11)  ndc11 ;
    //   string("\x01", maximum_length=14)  gpi14 ;
    //   string("\x01", maximum_length=1)  msc ;
    //   string("\x01", maximum_length=70)  drug_name ;
    //   string("\x01", maximum_length=30) prod_short_desc ;
    //   string("\x01", maximum_length=60) gpi14_desc ;
    //   string("\x01", maximum_length=2)  roa_cd ;
    //   string("\x01", maximum_length=4)  dosage_form_cd ;
    //   string("\x01", maximum_length=3)  rx_otc ;
    //   string("\x01", maximum_length=1)  repack_cd ;
    //   string("\x01", maximum_length=1)  status_cd ;
    //   string("\x01", maximum_length=8) inactive_dt ;
    //   decimal("\x01",0, maximum_length=39) alt_rank = NULL("") ;
    //   string("\x01", maximum_length=30) shared_qual ;
    //   string("\x01", maximum_length=20) override_tac_name = NULL("") ;
    //   string("\x01", maximum_length=20) override_tar_name = NULL("") ;
    //   string(2) constituent_group = NULL("");
    //   string(1) constituent_reqd = NULL("");
    //   decimal("\x01",0, maximum_length=39) constituent_rank = NULL("");
    //   string(1) newline = "\n";
    // end;
    // type tsd_enriched_data_t =
    // record
    //   decimal("\x01",0, maximum_length=10) tsd_id ;
    //   string("\x01", maximum_length=30) tsd_cd ;
    //   string("\x01", maximum_length=11)  ndc11 ;
    //   string("\x01", maximum_length=14)  gpi14 ;
    //   string("\x01", maximum_length=1)  msc ;
    //   string("\x01", maximum_length=70)  drug_name ;
    //   string("\x01", maximum_length=30) prod_short_desc ;
    //   string("\x01", maximum_length=60) gpi14_desc ;
    //   string("\x01", maximum_length=2)  roa_cd ;
    //   string("\x01", maximum_length=4)  dosage_form_cd ;
    //   string("\x01", maximum_length=3)  rx_otc ;
    //   string("\x01", maximum_length=1)  repack_cd ;
    //   string("\x01", maximum_length=1)  status_cd ;
    //   string("\x01", maximum_length=8) inactive_dt ;
    //   string(1) newline = "\n";
    // end;
    // type tac_enriched_data_t =
    // record
    //   decimal("\x01",0, maximum_length=10) tac_id ;
    //   string("\x01", maximum_length=20) tac_name ;
    //   decimal("\x01",0, maximum_length=39) priority ;
    //   string("\x01", maximum_length=20) Target_Alternative;
    //   string("\x01", maximum_length=11)  ndc11 ;
    //   string("\x01", maximum_length=14)  gpi14 ;
    //   string("\x01", maximum_length=1)  msc ;
    //   string("\x01", maximum_length=70)  drug_name ;
    //   string("\x01", maximum_length=30) prod_short_desc ;
    //   string("\x01", maximum_length=60) gpi14_desc ;
    //   string("\x01", maximum_length=2)  roa_cd ;
    //   string("\x01", maximum_length=4)  dosage_form_cd ;
    //   string("\x01", maximum_length=3)  rx_otc ;
    //   string("\x01", maximum_length=1)  repack_cd ;
    //   string("\x01", maximum_length=1)  status_cd ;
    //   string("\x01", maximum_length=8) inactive_dt ;
    //   string(1) newline = "\n";
    // end;
    // type tar_enriched_data_t =
    // record
    //   decimal("\x01",0, maximum_length=10) tar_id  ;
    //   string("\x01", maximum_length=20) tar_name ;
    //   string("\x01", maximum_length=20) Target_Alternative;
    //   string("\x01", maximum_length=11)  ndc11 ;
    //   string("\x01", maximum_length=14)  gpi14 ;
    //   string("\x01", maximum_length=1)  msc ;
    //   string("\x01", maximum_length=70)  drug_name ;
    //   string("\x01", maximum_length=30) prod_short_desc ;
    //   string("\x01", maximum_length=60) gpi14_desc ;
    //   string("\x01", maximum_length=2)  roa_cd ;
    //   string("\x01", maximum_length=4)  dosage_form_cd ;
    //   string("\x01", maximum_length=3)  rx_otc ;
    //   string("\x01", maximum_length=1)  repack_cd ;
    //   string("\x01", maximum_length=1)  status_cd ;
    //   string("\x01", maximum_length=8) inactive_dt ;
    //   string(1) newline = "\n";
    // end;
    // type asso_enriched_tac_data_t =
    // record
    //   decimal("\x01",0, maximum_length=10) tal_id ;
    //   string("\x01", maximum_length=20) tal_name ;
    //   string("\x01", maximum_length=20) tal_assoc_name = NULL("") ;
    //   string("\x01", maximum_length=20) Target_Alternative;
    //   string("\x01", maximum_length=11)  ndc11 ;
    //   string("\x01", maximum_length=14)  gpi14 ;
    //   string("\x01", maximum_length=1)  msc ;
    //   string("\x01", maximum_length=70)  drug_name ;
    //   string("\x01", maximum_length=30) prod_short_desc ;
    //   string("\x01", maximum_length=60) gpi14_desc ;
    //   string("\x01", maximum_length=2)  roa_cd ;
    //   string("\x01", maximum_length=4)  dosage_form_cd ;
    //   string("\x01", maximum_length=3)  rx_otc ;
    //   string("\x01", maximum_length=1)  repack_cd ;
    //   string("\x01", maximum_length=1)  status_cd ;
    //   string("\x01", maximum_length=8) inactive_dt ;
    //   string("\x01", maximum_length=30) shared_qual ;
    //   string("\x01", maximum_length=20) override_tac_name = NULL("") ;
    //   string("\x01", maximum_length=20) override_tar_name = NULL("") ;
    //   string(1) newline = "\n";
    // end;
    // type asso_enriched_sorted_data_t =
    // record
    //   decimal("\x01",0, maximum_length=10) tal_id ;
    //   string("\x01", maximum_length=20) tal_name ;
    //   string("\x01", maximum_length=20) tal_assoc_name = NULL("") ;
    //   string("\x01", maximum_length=11)  Target_ndc11 ='N/A';
    //   string("\x01", maximum_length=20) Target_Alternative;
    //   string("\x01", maximum_length=11)  ndc11 ;
    //   string("\x01", maximum_length=15) tad_eli_code= NULL("") ;
    //   string("\x01", maximum_length=14)  gpi14 ;
    //   string("\x01", maximum_length=1)  msc ;
    //   string("\x01", maximum_length=70)  drug_name ;
    //   string("\x01", maximum_length=30) prod_short_desc ;
    //   string("\x01", maximum_length=60) gpi14_desc ;
    //   string("\x01", maximum_length=2)  roa_cd ;
    //   string("\x01", maximum_length=4)  dosage_form_cd ;
    //   string("\x01", maximum_length=3)  rx_otc ;
    //   string("\x01", maximum_length=1)  repack_cd ;
    //   string("\x01", maximum_length=1)  status_cd ;
    //   string("\x01", maximum_length=8) inactive_dt ;
    //   string(1) newline = "\n";
    // end;
    // type asso_enriched_diff_data_t =
    // record
    //   string("\x01", maximum_length=20) Target_Alternative;
    //   string("\x01", maximum_length=11)  ndc11 ;
    //   string("\x01", maximum_length=14)  gpi14 ;
    //   string("\x01", maximum_length=1)  msc ;
    //   string("\x01", maximum_length=70)  drug_name ;
    //   string("\x01", maximum_length=30) prod_short_desc ;
    //   string("\x01", maximum_length=60) gpi14_desc ;
    //   string("\x01", maximum_length=2)  roa_cd ;
    //   string("\x01", maximum_length=4)  dosage_form_cd ;
    //   string("\x01", maximum_length=3)  rx_otc ;
    //   string("\x01", maximum_length=1)  repack_cd ;
    //   string("\x01", maximum_length=1)  status_cd ;
    //   string("\x01", maximum_length=8) inactive_dt ;
    //   end;
    // type udl_ref_t =
    // record
    //   date("YYYYMMDD")("\x01") as_of_dt ;
    // end;
    // type output_profile_rebate_dtl_t =
    // record
    //   decimal("\x01",0, maximum_length=10) output_profile_rebate_dtl_id ;
    //   string("\x01", maximum_length=20) udl_name ;
    //   decimal("\x01",0, maximum_length=10) output_profile_id ;
    //   string("\x01", maximum_length=15) rebate_elig_cd ;
    //   string(1) newline = "\n";
    // end;
    // type products_traversing_mapping_t = record
    //   bit_vector_t final_first_round_prdcts = allocate();
    //   bit_vector_t[int] final_first_round_sorted_prdcts = allocate();
    //   bit_vector_t first_round_prdcts = allocate();
    //   bit_vector_t[int] first_round_sorted_prdcts = allocate();
    //   bit_vector_t final_second_round_prdcts = allocate();
    //   bit_vector_t[int] final_second_round_sorted_prdcts = allocate();
    //   bit_vector_t second_round_prdcts  = allocate();
    //   bit_vector_t[int] second_round_sorted_prdcts = allocate();
    // end;
    // type store_bucket_idx =
    // record
    //   decimal("")index;
    //   decimal("")[int]bucket;
    // end;
    // type alt_contents_t =
    // record
    //   bit_vector_t[int] alt_prdcts;
    //   bit_vector_t[int] alt_prdcts_all_prio;
    //   bit_vector_t common_prdcts;
    // end;
    // type bucket_t =
    // record
    //   decimal('\x01') tar_content_index;
    //   decimal('\x01') alt_index;
    //   decimal('\x01') udl_index;
    //   bit_vector_t products = allocate();
    //   bit_vector_t sorted_prdcts_placeholder = allocate();
    //   bit_vector_t unsorted_prdcts_placeholder = allocate();
    //   bit_vector_t[int] final_sorted_prdcts = allocate();
    //   products_traversing_mapping_t sorted_prdcts = allocate();
    //   products_traversing_mapping_t unsorted_prdcts = allocate();
    //   string(2) constituent_group = NULL("");
    //   string(1) constituent_reqd = NULL("");
    //   string(20) udl_nm = NULL("");
    // end;
    // type tar_prdcts_mapping_t = record
    //  bucket_t[int] bucket;
    //  int[int] target_tar_index = [vector];
    // end;
    // type content_t =
    // record
    //     bit_vector_t[int] target_prdcts;
    // end;
    // type contents_row_t =
    // record
    //   bit_vector_t target_prdcts;
    //   bit_vector_t alt_prdcts;
    //   decimal(1) priority;
    //   decimal(1) ST_flag =0;
    // end;
    // type tac_contents_row_t =
    // record
    //   bit_vector_t target_prdcts;
    //   bit_vector_t alt_prdcts;
    //   contents_row_t[int] contents;
    //   decimal(1) xtra_proc_flg = 0;
    // end;
    // type alt_prdcts_rank_t =
    // record
    //   int alt_rank;
    //   int[int] alt_prdcts;
    //   string(2) constituent_group = NULL("");
    //   string(1) constituent_reqd = NULL("");
    // end;
    // let lookup_identifier_type tac_lkp = lookup_load($'TAC_EXP_XWALK',NULL,"LKP: TAC Exp");
    // let lookup_identifier_type tar_lkp = lookup_load($'TAR_EXP_XWALK',NULL,"LKP: TAR Exp");
    // let lookup_identifier_type prod_dtl = lookup_load($'PRODUCTS_FILE', NULL, "LKP: Prod");
    // let lookup_identifier_type rule_prod_dtl = lookup_load($'RULE_PRODUCTS_FILE', NULL, "LKP: Rule Products");
    // let lookup_identifier_type cag_udl_lkp = ($'CARRIER' == 'NULL')? lookup_load($'UDL_BASELINE_EXP_FILE',NULL,"Expanded UDL") : lookup_load($'UDL_CAG_EXP_FILE',NULL,"Expanded UDL");
    // let lookup_identifier_type baseline_udl_lkp = ($'CARRIER' != 'NULL')? lookup_load($'UDL_BASELINE_EXP_FILE',NULL,"Expanded UDL") : lookup_not_loaded();
    // let lookup_identifier_type cag_udl_lkp_wt_rule_priority = ($'CARRIER' == 'NULL')? lookup_load($'UDL_BASELINE_EXP_FILE_WT_RL_PRIORITY',NULL,"Expanded_UDL_wt_rl_priority") : lookup_load($'UDL_CAG_EXP_FILE_WT_RL_PRIORITY',NULL,"Expanded_UDL_wt_rl_priority");
    // let lookup_identifier_type baseline_udl_lkp_wt_rule_priority = ($'CARRIER' != 'NULL')? lookup_load($'UDL_BASELINE_EXP_FILE_WT_RL_PRIORITY',NULL,"Expanded_UDL_wt_rl_priority") : lookup_not_loaded();
    // type tar_xwalk_t =
    // record
    //   decimal("\x01",0, maximum_length=10) tar_id /*NUMBER(9) NOT NULL*/;
    //   decimal("\x01",0, maximum_length=10) tar_dtl_id /*NUMBER(9) NOT NULL*/;
    //   string("\x01", maximum_length=20) tar_name /*VARCHAR2(20) NOT NULL*/;
    //   decimal("\x01",0, maximum_length=39) tar_dtl_type_cd /*NUMBER(38) NOT NULL*/;
    //   record
    //     bit_vector_t target_prdcts;
    //     record
    //       bit_vector_t alt_prdcts;
    //       bit_vector_t alt_prdcts_all_prio;
    //       bit_vector_t common_prdcts;
    //     end[int] alt_contents;
    //   end[int] contents;
    //   bit_vector_t common_alt_prdcts;
    //   bit_vector_t common_target_prdcts;
    //   decimal(1) keep_all_targets = 0;
    //   string(1) newline = '\n';
    // end;
    //
    // out::reformat(in)=
    // begin
    // let tac_xwalk_t tac_dtls = allocate();
    // let bit_vector_t[int] target_prdcts = allocate();
    // let bit_vector_t[int] alt_prdcts = allocate();
    // let int indx = -1;
    // let tar_xwalk_t tar_prdcts = allocate();
    // let tar_prdcts_mapping_t[int] target_tar_prdcts_mapping = allocate();
    // let tar_prdcts_mapping_t[int] alt_tar_prdcts_mapping = allocate();
    // let target_alt_rank_t[int] target_prdct_dtl = allocate();
    // let target_alt_rank_t[int] unsorted_target_prdct_dtl = allocate();
    // let bucket_t[int] alt_bucket_vec = allocate();
    // let bucket_t[int] alt_bucket_vec_step = allocate();
    // let int[int] target_dl_bits = allocate();
    // let int[int] alt_dl_bits = allocate();
    // let int[int] tac_target_dl_bits = allocate();
    // let int[int] lv_tac_target_dl_bits = allocate();
    // let alt_rank_t[int] alt_rank_vec = allocate();
    // let contents_row_t[int] contents = allocate();
    // let tac_contents_row_t lv_tac_contents = allocate_with_defaults();
    // let bit_vector_t lv_target_prdcts = bv_all_zeros();
    // let bit_vector_t lv_alt_prdcts = bv_all_zeros();
    // let bit_vector_t _target_prdcts = bv_all_zeros();
    // let bit_vector_t _alt_prdcts = bv_all_zeros();
    // let bit_vector_t _tac_target_prdcts = bv_all_zeros();
    // let bit_vector_t _tac_alt_prdcts = bv_all_zeros();
    // let int pos;
    // let int[int] tar_idx_vec = [vector];
    // let bit_vector_t _all_tac_target_prdcts = bv_all_zeros();
    // let bit_vector_t _all_tac_alt_prdcts = bv_all_zeros();
    // let store_bucket_idx[int] store_bucket_idx_t = allocate();
    // let int[int] index_vec = [vector];
    // let alt_prdcts_rank_t[int] alt_prdcts_rank_vec = allocate();
    // let target_alt_rank_t[int] target_wo_alt_prdct_dtl = allocate();
    // let alt_constituent_prdct_t[int] alt_constituent_prdcts_vec = allocate();
    // let bit_vector_t step_alt_prdcts = allocate();
    //  let int length_of_contents=0;
    //  let int length_of_alt_contents=0;
    //  let alt_contents_t [int] tar_roa_df_alt=allocate();
    //  let tac_st_contents[int] steptal_prdcts=allocate();
    //  let tac_st_contents[int] steptal_inp=allocate();
    //  let bit_vector_t tar_st_target =bv_all_zeros();
    //  let bit_vector_t tar_prds = bv_all_zeros();
    //  let bit_vector_t step_alt_prds=bv_all_zeros();
    //  let bit_vector_t in_tar = bv_all_zeros();
    //  let bit_vector_t in_alt_step =bv_all_zeros();
    //  let bit_vector_t step_tar = bv_all_zeros();
    //  let bit_vector_t step_tar1 = bv_all_zeros();
    //  let bit_vector_t[int] final_sorted_prdcts1 = allocate();
    // let bit_vector_t[int] final_sorted_prdcts2 = allocate();
    // let bit_vector_t[int] final_sorted_prdcts_step = allocate();
    // let bit_vector_t[int] final_sorted_prdcts_step1 = allocate();
    // let bit_vector_t[int] final_sorted_prdcts_overlapping = allocate();
    // let number_of_alt_bucket = 0;
    // let st_len =0;
    // let length_overlapping = 0;
    // let bit_vector_t step_join_alternate=bv_all_zeros();
    // let bit_vector_t step_alt_prdcts_tala = allocate();
    //  tar_prdcts = if(in.tal_assoc_type_cd == 1 and is_defined(in.override_tar_name) and lookup_match(tar_lkp,"LKP: TAR Exp",in.override_tar_name)) lookup(tar_lkp,"LKP: TAR Exp",in.override_tar_name) else lookup(tar_lkp,"LKP: TAR Exp",$'TAR_NM');
    //  tar_roa_df_alt=tar_prdcts.contents[0].alt_contents;
    //  if(in.tal_id==0)
    //  begin
    //  step_alt_prdcts=in.alt_constituent_prdcts[0].alt_prdcts;
    //  length_of_contents=length_of(tar_prdcts.contents);
    //  for(let c=0 , c < length_of_contents)
    //  begin
    //  length_of_alt_contents=length_of(tar_prdcts.contents[c].alt_contents);
    //  for( let a=0, a < length_of_alt_contents)
    //  begin
    //   tar_prdcts.contents[c].alt_contents[a].alt_prdcts=vector_append(tar_prdcts.contents[c].alt_contents[a].alt_prdcts,step_alt_prdcts);
    //  tar_prdcts.contents[0].alt_contents[a].common_prdcts=bv_or(tar_prdcts.contents[0].alt_contents[a].common_prdcts,step_alt_prdcts);
    //  end
    //  end
    //  end
    //   if( in.tal_assoc_type_cd == 1)
    //   begin
    //      tac_dtls = if(is_defined(in.override_tac_name) and lookup_match(tac_lkp,"LKP: TAC Exp",in.override_tac_name)) lookup(tac_lkp,"LKP: TAC Exp",in.override_tac_name) else lookup(tac_lkp,"LKP: TAC Exp",$'TAC_NM');
    //         for( let i , i < length_of(tac_dtls.tac_contents) )
    //         begin
    //             lv_target_prdcts = if(tar_prdcts.keep_all_targets) tac_dtls.tac_contents[i].target_prdcts else  bv_and(tac_dtls.tac_contents[i].target_prdcts,tar_prdcts.common_target_prdcts);
    //             lv_alt_prdcts    = bv_and(tac_dtls.tac_contents[i].alt_prdcts,tar_prdcts.common_alt_prdcts);
    //             if ( bv_count_one_bits(lv_target_prdcts) and bv_count_one_bits(lv_alt_prdcts) )
    //             begin
    //                 if(tac_dtls.tac_contents[i].xtra_proc_flg)
    //                 begin
    //                   for(let rec in tac_dtls.tac_contents[i].contents)
    //                   begin
    //                     _target_prdcts = bv_and(lv_target_prdcts,rec.target_prdcts);
    //                     _alt_prdcts    = bv_and(lv_alt_prdcts,rec.alt_prdcts);
    //                     if( bv_count_one_bits(_target_prdcts) and bv_count_one_bits(_alt_prdcts) )
    //                     begin
    //                       contents           = vector_append(contents,[record target_prdcts _target_prdcts alt_prdcts _alt_prdcts ST_flag rec.ST_flag priority rec.priority] );
    //                       _tac_target_prdcts = bv_or(_tac_target_prdcts,_target_prdcts);
    //                       _tac_alt_prdcts    = bv_or(_tac_alt_prdcts,_alt_prdcts);
    //                     end
    //                   end
    //                   if( bv_count_one_bits(_tac_target_prdcts) and bv_count_one_bits(_tac_alt_prdcts) )
    //                   begin
    //                      _all_tac_target_prdcts = bv_or(_all_tac_target_prdcts, _tac_target_prdcts);
    //                      _all_tac_alt_prdcts    = bv_or(_all_tac_alt_prdcts, _tac_alt_prdcts);
    //                      _tac_target_prdcts     = bv_all_zeros();
    //                      _tac_alt_prdcts        = bv_all_zeros();
    //                   end
    //                 end
    //                 else
    //                 begin
    //                    contents               = vector_append(contents,[record target_prdcts lv_target_prdcts alt_prdcts lv_alt_prdcts ST_flag tac_dtls.tac_contents[i].contents[0].ST_flag priority tac_dtls.tac_contents[i].contents[0].priority]);
    //                    _all_tac_target_prdcts = bv_or(_all_tac_target_prdcts, lv_target_prdcts);
    //                    _all_tac_alt_prdcts    = bv_or(_all_tac_alt_prdcts, lv_alt_prdcts);
    //                 end
    //             end
    //          end
    //          if(bv_count_one_bits(_all_tac_target_prdcts))
    //          begin
    //              lv_tac_contents.target_prdcts = _all_tac_target_prdcts;
    //              lv_tac_contents.alt_prdcts    = _all_tac_alt_prdcts;
    //              lv_tac_contents.contents      = contents;
    //              lv_tac_contents.xtra_proc_flg = (length_of(contents) > 1);
    //          end
    //          else
    //            force_error("Applied TAR on TAC - No Products Left For Processing");
    //       for ( let target_prds in in.target_prdcts )
    //         target_prdcts = vector_append( target_prdcts, bv_and(target_prds, _all_tac_target_prdcts));
    //       if ( !bv_count_one_bits( bv_vector_or(target_prdcts) ) )
    //          force_error("TAC Applied - No Target Products Left For Processing");
    //       if ( length_of(in.alt_constituent_prdcts) )
    //       begin
    //          for( let alt_consti_prdcts in in.alt_constituent_prdcts )
    //              alt_constituent_prdcts_vec = vector_append( alt_constituent_prdcts_vec,[ record alt_prdcts bv_and( alt_consti_prdcts.alt_prdcts, _all_tac_alt_prdcts)
    //                                                                                              constituent_group alt_consti_prdcts.constituent_group
    //                                                                                              constituent_reqd alt_consti_prdcts.constituent_reqd
    //                                                                                              udl_nm alt_consti_prdcts.udl_nm]);
    //          target_tar_prdcts_mapping = apply_tar(target_prdcts,alt_constituent_prdcts_vec,tar_prdcts,0,target_tar_prdcts_mapping,'',0);
    //          alt_tar_prdcts_mapping = apply_tar([vector],alt_constituent_prdcts_vec,tar_prdcts,1,target_tar_prdcts_mapping,in.shared_qual,0);
    //         if(in.shared_qual == 'N/A')
    //          begin
    //             for(let target_prd in target_tar_prdcts_mapping)
    //             begin
    //                let bucket_index = 0;
    //                for(let bucket in target_prd.bucket)
    //                begin
    //                   bucket_index = bucket_index + 1;
    //                   number_of_alt_bucket=0;
    //                   length_overlapping=0;
    //                   step_join_alternate=allocate();
    //                   final_sorted_prdcts_step= allocate();
    //                   for(let alt_prd in alt_tar_prdcts_mapping)
    //                   begin
    //                      alt_bucket_vec = vector_concat(alt_bucket_vec,vector_select(alt_prd.bucket,[record tar_content_index bucket.tar_content_index alt_index 0 udl_index 0 products bv_all_zeros() sorted_prdcts_placeholder bv_all_zeros() unsorted_prdcts_placeholder bv_all_zeros() final_sorted_prdcts_dl_bits [vector]
    //                                                                               sorted_prdcts [record final_first_round_prdcts bv_all_zeros() final_first_round_prdcts_dl_bits [vector] first_round_prdcts bv_all_zeros() first_round_prdcts_dl_bits [vector] final_second_round_prdcts bv_all_zeros() final_second_round_prdcts_dl_bits [vector] second_round_prdcts bv_all_zeros() second_round_prdcts_dl_bits [vector]]
    //                                                                               unsorted_prdcts [record final_first_round_prdcts bv_all_zeros() final_first_round_prdcts_dl_bits [vector] first_round_prdcts bv_all_zeros() first_round_prdcts_dl_bits [vector] final_second_round_prdcts bv_all_zeros() final_second_round_prdcts_dl_bits [vector] second_round_prdcts bv_all_zeros() second_round_prdcts_dl_bits [vector]]],${tar_content_index}));
    //                   end
    //                   for(let prds in bucket.final_sorted_prdcts) target_dl_bits = vector_concat(target_dl_bits, for (let p in bv_indices(prds) ): p);
    //                   if(length_of(alt_bucket_vec))
    //                   begin
    //                      alt_bucket_vec = vector_sort(alt_bucket_vec, {tar_content_index;alt_index;udl_index});
    //                      unsorted_target_prdct_dtl= apply_gpi14_sorting(target_dl_bits,bucket_index,bucket.udl_index,bucket.products,alt_bucket_vec,lv_tac_contents,tar_roa_df_alt,steptal_inp,in.tar_udl_nm,final_sorted_prdcts_step);
    //                      for(let p in target_dl_bits)
    //                      begin
    //                         let idx = vector_search(unsorted_target_prdct_dtl,[record target_dl_bit p bucket_index bucket_index udl_index bucket.udl_index tac_index 0 alt_prdcts_rank [vector] has_alt '' ST_flag 0],${target_dl_bit});
    //                           if( unsorted_target_prdct_dtl[idx].has_alt == 'Y' )
    //                             target_prdct_dtl = vector_append(target_prdct_dtl, unsorted_target_prdct_dtl[idx]);
    //                           else
    //                             target_wo_alt_prdct_dtl = vector_append(target_wo_alt_prdct_dtl, unsorted_target_prdct_dtl[idx]);
    //                      end
    //                      alt_bucket_vec = allocate();
    //                   end
    //                   else
    //                   begin
    //                      for(let p in target_dl_bits)
    //                         target_wo_alt_prdct_dtl = vector_append(target_wo_alt_prdct_dtl,[record target_dl_bit p bucket_index 0 udl_index 0 tac_index 0 alt_prdcts_rank [vector] has_alt 'N' ST_flag 0]);
    //                   end
    //                   target_dl_bits = allocate();
    //                 end
    //              end
    //          end
    //          else
    //          begin
    //             for (let y, y < length_of(alt_tar_prdcts_mapping) )
    //             begin
    //                tar_idx_vec = vector_sort_dedup_first(alt_tar_prdcts_mapping[y].target_tar_index);
    //                store_bucket_idx_t = vector_append( store_bucket_idx_t , [ record index y bucket [vector] ] );
    //                index_vec = vector_append(index_vec, y);
    //                for( let z, z < length_of(tar_idx_vec) )
    //                begin
    //                   pos = vector_search(target_tar_prdcts_mapping[y].target_tar_index,tar_idx_vec[z]);
    //                   target_dl_bits = allocate();
    //                   alt_bucket_vec = vector_select(alt_tar_prdcts_mapping[y].bucket,[record tar_content_index tar_idx_vec[z] alt_index 0 udl_index y products bv_all_zeros() sorted_prdcts_placeholder bv_all_zeros() unsorted_prdcts_placeholder bv_all_zeros() final_sorted_prdcts_dl_bits [vector]
    //                                                          sorted_prdcts [record final_first_round_prdcts bv_all_zeros() final_first_round_prdcts_dl_bits [vector] first_round_prdcts bv_all_zeros() first_round_prdcts_dl_bits [vector] final_second_round_prdcts bv_all_zeros() final_second_round_prdcts_dl_bits [vector] second_round_prdcts bv_all_zeros() second_round_prdcts_dl_bits [vector]]
    //                                                          unsorted_prdcts [record final_first_round_prdcts bv_all_zeros() final_first_round_prdcts_dl_bits [vector] first_round_prdcts bv_all_zeros() first_round_prdcts_dl_bits [vector] final_second_round_prdcts bv_all_zeros() final_second_round_prdcts_dl_bits [vector] second_round_prdcts bv_all_zeros() second_round_prdcts_dl_bits [vector]]],{tar_content_index;udl_index});
    //                   for(let prds in target_tar_prdcts_mapping[y].bucket[pos].final_sorted_prdcts)
    //                   begin
    //                      target_dl_bits = vector_concat(target_dl_bits, for (let p in bv_indices(prds) ): p);
    //                      store_bucket_idx_t[y].bucket = vector_append(store_bucket_idx_t[y].bucket,pos);
    //                   end
    //                   if(in.shared_qual !='GPI14')
    //                   begin
    //                      unsorted_target_prdct_dtl = apply_gpi14_sorting(target_dl_bits,pos+1,target_tar_prdcts_mapping[y].bucket[pos].udl_index,target_tar_prdcts_mapping[y].bucket[pos].products,alt_bucket_vec,lv_tac_contents,tar_roa_df_alt,steptal_inp,in.tar_udl_nm,final_sorted_prdcts_step);
    //                      for(let p in target_dl_bits)
    //                      begin
    //                         let idx = vector_search(unsorted_target_prdct_dtl,[record target_dl_bit p bucket_index 0 udl_index 0 tac_index 0 alt_prdcts_rank [vector] has_alt '' ST_flag 0],${target_dl_bit});
    //                           if( unsorted_target_prdct_dtl[idx].has_alt == 'Y' )
    //                             target_prdct_dtl = vector_append(target_prdct_dtl, unsorted_target_prdct_dtl[idx]);
    //                           else
    //                             target_wo_alt_prdct_dtl = vector_append(target_wo_alt_prdct_dtl, unsorted_target_prdct_dtl[idx]);
    //                      end
    //                      alt_bucket_vec = allocate();
    //                   end
    //                   else
    //                   begin
    //                      let int alt_rank = 1;
    //                      let tac_index = 0;
    //                      let bit_vector_t unmatching_udl_prd;
    //                      tac_target_dl_bits = target_dl_bits;
    //                      begin block exit_block
    //                         for(let rec in lv_tac_contents.contents)
    //                         begin
    //                            if(length_of(tac_target_dl_bits))
    //                            begin
    //                               lv_tac_target_dl_bits = bv_indices(bv_and(target_tar_prdcts_mapping[y].bucket[pos].products,rec.target_prdcts));
    //                               if(length_of(lv_tac_target_dl_bits))
    //                               begin
    //                                 for(let bucket1 in alt_bucket_vec)
    //                                  begin
    //                                     if(bv_count_one_bits(bucket1.sorted_prdcts_placeholder) or bv_count_one_bits(bucket1.unsorted_prdcts_placeholder))
    //                                     begin
    //                                           for( let alt_prds in bucket1.final_sorted_prdcts)
    //                                           begin
    //                                            let bit_vector_t[int] tar_roa_df_alt_cntnt=tar_roa_df_alt[bucket1.tar_content_index].alt_prdcts_all_prio[bucket1.alt_index];
    //                                             unmatching_udl_prd =bv_and(alt_prds,rec.alt_prdcts);
    //                                             alt_dl_bits = apply_udl_sort(unmatching_udl_prd,bucket1.udl_nm,tar_roa_df_alt_cntnt,0,1);
    //                                             if(length_of(alt_dl_bits))
    //                                             begin
    //                                                alt_rank_vec = vector_append(alt_rank_vec, [record alt_rank alt_rank alt_prdcts alt_dl_bits udl_nm bucket1.udl_nm]);
    //                                                alt_rank = alt_rank + length_of(bucket1.final_sorted_prdcts);
    //                                             end
    //                                           end
    //                                     end
    //                                  end
    //                                  if(length_of(alt_rank_vec))
    //                                  begin
    //                                     tac_index = tac_index + 1;
    //                                     for(let p in lv_tac_target_dl_bits)
    //                                       unsorted_target_prdct_dtl = vector_append(unsorted_target_prdct_dtl, [record target_dl_bit p bucket_index pos+1 udl_index target_tar_prdcts_mapping[y].bucket[pos].udl_index tac_index tac_index alt_prdcts_rank alt_rank_vec rebate_elig_contents [vector] has_alt 'Y' ST_flag 0]);
    //                                     alt_rank_vec = allocate();
    //                                     tac_target_dl_bits = vector_difference(tac_target_dl_bits,lv_tac_target_dl_bits);
    //                                     alt_rank = 1;
    //                                  end
    //                              end
    //                           end
    //                           else exit exit_block;
    //                         end
    //                      end block exit_block;
    //                      for ( let p in tac_target_dl_bits )
    //                        target_wo_alt_prdct_dtl = vector_append(target_wo_alt_prdct_dtl, [record target_dl_bit p bucket_index pos+1 udl_index target_tar_prdcts_mapping[y].bucket[pos].udl_index alt_prdcts_rank [vector] rebate_elig_contents [vector] has_alt 'N' ST_flag 0]);
    //                      for(let p in target_dl_bits)
    //                        target_prdct_dtl = vector_concat(target_prdct_dtl, vector_select(unsorted_target_prdct_dtl,[record target_dl_bit p bucket_index 0 udl_index 0 tac_index 0 alt_prdcts_rank [vector] has_alt '' ST_flag 0],${target_dl_bit}));
    //                      alt_bucket_vec = allocate();
    //                         end
    //                    end
    //              end
    //              for( let int i , i < length_of(target_tar_prdcts_mapping))
    //              begin
    //                if ( i member index_vec )
    //                begin
    //                  for ( let int j , j < length_of(target_tar_prdcts_mapping[i].bucket) )
    //                     if ( j not member store_bucket_idx_t[i].bucket )
    //                     begin
    //                       for ( let prod_grp in target_tar_prdcts_mapping[i].bucket[j].final_sorted_prdcts )
    //                          target_wo_alt_prdct_dtl = vector_concat( target_wo_alt_prdct_dtl,for(let p in bv_indices(prod_grp)) : [ record target_dl_bit p  bucket_index 0 udl_index 0 alt_prdcts_rank [vector] has_alt 'N' ST_flag 0] );
    //                     end;
    //                end;
    //                else
    //                  for ( let int j , j < length_of(target_tar_prdcts_mapping[i].bucket) )
    //                    target_wo_alt_prdct_dtl = vector_concat( target_wo_alt_prdct_dtl, for(let p in bv_indices(bv_vector_or(target_tar_prdcts_mapping[i].bucket[j].final_sorted_prdcts))) : [ record target_dl_bit p bucket_index 0 udl_index 0 alt_prdcts_rank [vector] has_alt 'N' ST_flag 0]);
    //              end;
    //          end;
    //       end;
    //       else
    //       begin
    //         target_wo_alt_prdct_dtl = vector_concat( target_wo_alt_prdct_dtl, for(let p in bv_indices(bv_vector_or(target_prdcts))) : [ record target_dl_bit p bucket_index 0 udl_index 0 alt_prdcts_rank [vector] has_alt 'N' ST_flag 0] );
    //       end;
    //   end
    //   else
    //   begin
    //      let alt_rank = 1;
    //       target_tar_prdcts_mapping = apply_tar(in.target_prdcts,in.alt_constituent_prdcts,tar_prdcts,0,target_tar_prdcts_mapping,'',0);
    //       alt_tar_prdcts_mapping = apply_tar([vector],in.alt_constituent_prdcts,tar_prdcts,1,target_tar_prdcts_mapping,in.shared_qual,1);
    //       if(in.shared_qual != 'N/A')
    //       begin
    //          for (let a, a < length_of(alt_tar_prdcts_mapping) )
    //          begin
    //              for(let bucket in alt_tar_prdcts_mapping[a].bucket )
    //              begin
    //                 let bit_vector_t[int] tar_roa_df_alt_cntnt=tar_roa_df_alt[bucket.tar_content_index].alt_prdcts_all_prio[bucket.alt_index];
    //                 for(let prds in bucket.final_sorted_prdcts )
    //                 begin
    //                    alt_prdcts_rank_vec =  vector_append(alt_prdcts_rank_vec, [record alt_rank alt_rank alt_prdcts apply_udl_sort(prds,bucket.udl_nm,tar_roa_df_alt_cntnt,0,1)]);
    //                    alt_rank = alt_rank + bv_count_one_bits(prds);
    //                 end
    //              end
    //              target_prdct_dtl = vector_concat(target_prdct_dtl,  for(let prd in bv_indices(in.target_prdcts[a])): [record target_dl_bit prd bucket_index 0 udl_index a+1 alt_prdcts_rank alt_prdcts_rank_vec has_alt 'Y' ST_flag 0]);
    //              alt_prdcts_rank_vec = allocate();
    //              alt_rank = 1;
    //          end
    //          for(let b= length_of(alt_tar_prdcts_mapping), b < length_of(in.target_prdcts) )
    //             target_wo_alt_prdct_dtl = vector_concat(target_wo_alt_prdct_dtl, for(let prd in bv_indices(in.target_prdcts[b])): [record target_dl_bit prd bucket_index 0 udl_index 0 alt_prdcts_rank [vector] has_alt 'N' ST_flag 0]);
    //       end
    //       else
    //       begin
    //          for(let alt_prd in alt_tar_prdcts_mapping)
    //          begin
    //               for(let bucket in alt_prd.bucket )
    //               begin
    //                  let bit_vector_t[int] tar_roa_df_alt_cntnt=tar_roa_df_alt[bucket.tar_content_index].alt_prdcts_all_prio[bucket.alt_index];
    //                  for(let prds in bucket.final_sorted_prdcts )
    //                  begin
    //                     alt_prdcts_rank_vec = vector_append(alt_prdcts_rank_vec, [record alt_rank alt_rank alt_prdcts apply_udl_sort(prds,bucket.udl_nm,tar_roa_df_alt_cntnt,0,1) constituent_group bucket.constituent_group constituent_reqd  bucket.constituent_reqd]);
    //                     alt_rank = alt_rank + bv_count_one_bits(prds);
    //                  end
    //               end
    //          end
    //           if( length_of(alt_prdcts_rank_vec))
    //           begin
    //               let udl_index = 0;
    //               for(let target_prd in in.target_prdcts)
    //               begin
    //                 udl_index = udl_index + 1;
    //                 target_prdct_dtl = vector_concat(target_prdct_dtl, for(let prd in bv_indices(target_prd)): [record target_dl_bit prd bucket_index 0 udl_index udl_index alt_prdcts_rank alt_prdcts_rank_vec has_alt 'Y' ST_flag 0]);
    //               end
    //           end
    //           else
    //           begin
    //               for(let target_prd in in.target_prdcts)
    //                 target_wo_alt_prdct_dtl = vector_concat(target_wo_alt_prdct_dtl, for(let prd in bv_indices(target_prd)): [record target_dl_bit prd alt_prdcts_rank [vector] has_alt 'N' ST_flag 0]);
    //           end
    //       end
    //   end
    //   out.* :: in.*;
    //   out.ta_prdct_dtls :: target_prdct_dtl;
    //   out.ta_prdct_dtls_wo_alt :: target_wo_alt_prdct_dtl;
    // end;
    // out::apply_tar(udl_prdcts,alt_udl_prdcts,tar_prdcts,is_alt,target_tar_prdcts_mapping, shared_qualifier,is_override_incl) =
    // begin
    // let tar_prdcts_mapping_t[int] tar_prdcts_mapping = allocate();
    // let bit_vector_t _prdcts = bv_all_zeros();
    // let bit_vector_t lv_prdcts = bv_all_zeros();
    // let bit_vector_t left_over_prdcts = allocate();
    // let content_t[int] contents = allocate();
    // let len = 0;
    // let udl_len = 0;
    // if(is_alt)
    // begin
    //   len = length_of(tar_prdcts.contents[0].alt_contents);
    //   udl_len = length_of(alt_udl_prdcts);
    //   for ( let j=1, j < length_of(tar_prdcts.contents) )
    //     contents = vector_append(contents, [record target_prdcts tar_prdcts.contents[j].alt_contents[0].alt_prdcts]);
    // end
    // else
    // begin
    //    len = length_of(tar_prdcts.contents[0].target_prdcts);
    //    udl_len = length_of(udl_prdcts);
    //    contents = vector_slice(tar_prdcts.contents,1,length_of(tar_prdcts.contents)-1);
    // end
    //  for (let i, i < udl_len)
    //  begin
    //   _prdcts = bv_all_zeros();
    //   tar_prdcts_mapping = vector_append(tar_prdcts_mapping, [record bucket [vector]]);
    //   if(is_alt)
    //   begin
    //     if(shared_qualifier == 'N/A')
    //     begin
    //       let bit_vector_t udl_prdts = alt_udl_prdcts[i].alt_prdcts;
    //        for( let p, p < length_of(target_tar_prdcts_mapping) )
    //        begin
    //         for (let idx in target_tar_prdcts_mapping[p].target_tar_index)
    //         begin
    //             if( is_override_incl )
    //            begin
    //               _prdcts = bv_and(udl_prdts,tar_prdcts.contents[0].alt_contents[idx].common_prdcts);
    //               udl_prdts = bv_difference(udl_prdts,_prdcts);
    //            end
    //            else
    //               _prdcts = bv_and(alt_udl_prdcts[i].alt_prdcts,tar_prdcts.contents[0].alt_contents[idx].common_prdcts);
    //            if(bv_count_one_bits(_prdcts))
    //            begin
    //               let bit_vector_t[int] alt_prds = tar_prdcts.contents[0].alt_contents[idx].alt_prdcts;
    //               let alt_len = length_of(alt_prds);
    //               begin block stop_block
    //                 for(let n, n < alt_len)
    //                 begin
    //                    lv_prdcts = bv_and ( _prdcts, alt_prds[n] );
    //                    if ( bv_count_one_bits( lv_prdcts ) )
    //                    begin
    //                      tar_prdcts_mapping[i].target_tar_index = vector_append(tar_prdcts_mapping[i].target_tar_index,idx);
    //                      tar_prdcts_mapping[i].bucket = vector_append(tar_prdcts_mapping[i].bucket, [record tar_content_index idx alt_index n udl_index i products lv_prdcts sorted_prdcts_placeholder bv_all_zeros() unsorted_prdcts_placeholder bv_all_zeros() final_sorted_prdcts [vector]
    //                                                                                                      sorted_prdcts [record final_first_round_prdcts bv_all_zeros() final_first_round_sorted_prdcts [vector] first_round_prdcts bv_all_zeros() first_round_sorted_prdcts [vector] final_second_round_prdcts bv_all_zeros() final_second_round_sorted_prdcts [vector] second_round_prdcts bv_all_zeros() second_round_prdcts_dl_bits [vector]]
    //                                                                                                      unsorted_prdcts [record final_first_round_prdcts bv_all_zeros() final_first_round_sorted_prdcts [vector] first_round_prdcts bv_all_zeros() first_round_sorted_prdcts [vector] final_second_round_prdcts bv_all_zeros() final_second_round_sorted_prdcts [vector] second_round_prdcts bv_all_zeros() second_round_prdcts_dl_bits [vector]]
    //                                                                                                      constituent_group alt_udl_prdcts[i].constituent_group  constituent_reqd alt_udl_prdcts[i].constituent_reqd udl_nm alt_udl_prdcts[i].udl_nm]);
    //                      _prdcts = bv_difference(_prdcts, lv_prdcts);
    //                      if(!bv_count_one_bits(_prdcts))
    //                        exit stop_block;
    //                    end
    //                 end
    //               end block stop_block;
    //           end
    //         end
    //        end
    //          if( is_override_incl and bv_count_one_bits(udl_prdts))
    //             tar_prdcts_mapping[i].bucket = vector_append(tar_prdcts_mapping[i].bucket, [record tar_content_index length_of(tar_prdcts_mapping[i].bucket) alt_index 0 udl_index i products udl_prdts sorted_prdcts_placeholder bv_all_zeros() unsorted_prdcts_placeholder bv_all_zeros() final_sorted_prdcts [vector]
    //                                                                                                sorted_prdcts [record final_first_round_prdcts bv_all_zeros() final_first_round_sorted_prdcts [vector] first_round_prdcts bv_all_zeros() first_round_sorted_prdcts [vector] final_second_round_prdcts bv_all_zeros() final_second_round_sorted_prdcts [vector] second_round_prdcts bv_all_zeros() second_round_sorted_prdcts [vector]]
    //                                                                                                unsorted_prdcts [record final_first_round_prdcts bv_all_zeros() final_first_round_sorted_prdcts [vector] first_round_prdcts bv_all_zeros() first_round_sorted_prdcts [vector] final_second_round_prdcts bv_all_zeros() final_second_round_sorted_prdcts [vector] second_round_prdcts bv_all_zeros() second_round_sorted_prdcts [vector]]
    //                                                                                                constituent_group alt_udl_prdcts[i].constituent_group  constituent_reqd alt_udl_prdcts[i].constituent_reqd udl_nm alt_udl_prdcts[i].udl_nm]);
    //     end
    //     else
    //     begin
    //       let bit_vector_t udl_prdts = alt_udl_prdcts[i].alt_prdcts;
    //       for(let idx in target_tar_prdcts_mapping[i].target_tar_index)
    //       begin
    //         if( is_override_incl )
    //           begin
    //              _prdcts = bv_and(udl_prdts,tar_prdcts.contents[0].alt_contents[idx].common_prdcts);
    //              udl_prdts = bv_difference(udl_prdts,_prdcts);
    //           end
    //           else
    //              _prdcts = bv_and(alt_udl_prdcts[i].alt_prdcts,tar_prdcts.contents[0].alt_contents[idx].common_prdcts);
    //         if(bv_count_one_bits(_prdcts))
    //         begin
    //            let bit_vector_t[int] alt_prds = tar_prdcts.contents[0].alt_contents[idx].alt_prdcts;
    //            let alt_len = length_of(alt_prds);
    //           begin block stop_block1
    //              for(let n, n < alt_len)
    //              begin
    //                 lv_prdcts = bv_and ( _prdcts, alt_prds[n] );
    //                 if ( bv_count_one_bits( lv_prdcts ) )
    //                 begin
    //                   tar_prdcts_mapping[i].target_tar_index = vector_append(tar_prdcts_mapping[i].target_tar_index,idx);
    //                   tar_prdcts_mapping[i].bucket = vector_append(tar_prdcts_mapping[i].bucket, [record tar_content_index idx alt_index n udl_index i products lv_prdcts sorted_prdcts_placeholder bv_all_zeros() unsorted_prdcts_placeholder bv_all_zeros() final_sorted_prdcts [vector]
    //                                                                                                   sorted_prdcts [record final_first_round_prdcts bv_all_zeros() final_first_round_sorted_prdcts [vector] first_round_prdcts bv_all_zeros() first_round_sorted_prdcts [vector] final_second_round_prdcts bv_all_zeros() final_second_round_sorted_prdcts [vector] second_round_prdcts bv_all_zeros() second_round_prdcts_dl_bits [vector]]
    //                                                                                                   unsorted_prdcts [record final_first_round_prdcts bv_all_zeros() final_first_round_sorted_prdcts [vector] first_round_prdcts bv_all_zeros() first_round_sorted_prdcts [vector] final_second_round_prdcts bv_all_zeros() final_second_round_sorted_prdcts [vector] second_round_prdcts bv_all_zeros() second_round_prdcts_dl_bits [vector]]
    //                                                                                                   constituent_group alt_udl_prdcts[i].constituent_group  constituent_reqd alt_udl_prdcts[i].constituent_reqd udl_nm alt_udl_prdcts[i].udl_nm]);
    //                   _prdcts = bv_difference(_prdcts, lv_prdcts);
    //                   if(!bv_count_one_bits(_prdcts))
    //                     exit stop_block1;
    //                 end
    //              end
    //           end block stop_block1;
    //         end
    //      end
    //       if( is_override_incl and bv_count_one_bits(udl_prdts))
    //          tar_prdcts_mapping[i].bucket = vector_append(tar_prdcts_mapping[i].bucket, [record tar_content_index length_of(tar_prdcts_mapping[i].bucket) alt_index 0 udl_index i products udl_prdts sorted_prdcts_placeholder bv_all_zeros() unsorted_prdcts_placeholder bv_all_zeros() final_sorted_prdcts [vector]
    //                                                                                             sorted_prdcts [record final_first_round_prdcts bv_all_zeros() final_first_round_sorted_prdcts [vector] first_round_prdcts bv_all_zeros() first_round_sorted_prdcts [vector] final_second_round_prdcts bv_all_zeros() final_second_round_sorted_prdcts [vector] second_round_prdcts bv_all_zeros() second_round_sorted_prdcts [vector]]
    //                                                                                             unsorted_prdcts [record final_first_round_prdcts bv_all_zeros() final_first_round_sorted_prdcts [vector] first_round_prdcts bv_all_zeros() first_round_sorted_prdcts [vector] final_second_round_prdcts bv_all_zeros() final_second_round_sorted_prdcts [vector] second_round_prdcts bv_all_zeros() second_round_sorted_prdcts [vector]]
    //                                                                                             constituent_group alt_udl_prdcts[i].constituent_group  constituent_reqd alt_udl_prdcts[i].constituent_reqd udl_nm alt_udl_prdcts[i].udl_nm]);
    //     end
    //   end
    //   else
    //   begin
    //       _prdcts = udl_prdcts[i];
    //     begin block exit_block
    //      for ( let m, m < len )
    //      begin
    //           lv_prdcts = bv_and ( _prdcts, tar_prdcts.contents[0].target_prdcts[m] );
    //           if ( bv_count_one_bits( lv_prdcts ) )
    //           begin
    //             tar_prdcts_mapping[i].target_tar_index = vector_append(tar_prdcts_mapping[i].target_tar_index,m);
    //             tar_prdcts_mapping[i].bucket = vector_append(tar_prdcts_mapping[i].bucket, [record tar_content_index m alt_index 0 udl_index i products lv_prdcts sorted_prdcts_placeholder bv_all_zeros() unsorted_prdcts_placeholder bv_all_zeros() final_sorted_prdcts [vector]
    //                                                                                             sorted_prdcts [record final_first_round_prdcts bv_all_zeros() final_first_round_sorted_prdcts [vector] first_round_prdcts bv_all_zeros() first_round_sorted_prdcts [vector] final_second_round_prdcts bv_all_zeros() final_second_round_sorted_prdcts [vector] second_round_prdcts bv_all_zeros() second_round_prdcts_dl_bits [vector]]
    //                                                                                             unsorted_prdcts [record final_first_round_prdcts bv_all_zeros() final_first_round_sorted_prdcts [vector] first_round_prdcts bv_all_zeros() first_round_sorted_prdcts [vector] final_second_round_prdcts bv_all_zeros() final_second_round_sorted_prdcts [vector] second_round_prdcts bv_all_zeros() second_round_prdcts_dl_bits [vector]]]);
    //             _prdcts = bv_difference(_prdcts, lv_prdcts);
    //             if(!bv_count_one_bits(_prdcts))
    //                exit exit_block;
    //           end
    //       end
    //       if(bv_count_one_bits(_prdcts))
    //           left_over_prdcts = _prdcts;
    //      if(bv_count_one_bits(left_over_prdcts))
    //      begin
    //         _prdcts = if(bv_count_one_bits(contents[0].target_prdcts[0])) bv_and ( left_over_prdcts, contents[0].target_prdcts[0] ) else left_over_prdcts;
    //          if ( bv_count_one_bits( _prdcts ) )
    //           tar_prdcts_mapping[i].bucket = vector_append(tar_prdcts_mapping[i].bucket, [record tar_content_index len alt_index 0 udl_index i products _prdcts sorted_prdcts_placeholder bv_all_zeros() unsorted_prdcts_placeholder bv_all_zeros() final_sorted_prdcts [vector]
    //                                                                                                         sorted_prdcts [record final_first_round_prdcts bv_all_zeros() final_first_round_sorted_prdcts [vector] first_round_prdcts bv_all_zeros() first_round_sorted_prdcts [vector] final_second_round_prdcts bv_all_zeros() final_second_round_sorted_prdcts [vector] second_round_prdcts bv_all_zeros() second_round_sorted_prdcts [vector]]
    //                                                                                                         unsorted_prdcts [record final_first_round_prdcts bv_all_zeros() final_first_round_sorted_prdcts [vector] first_round_prdcts bv_all_zeros() first_round_sorted_prdcts [vector] final_second_round_prdcts bv_all_zeros() final_second_round_sorted_prdcts [vector] second_round_prdcts bv_all_zeros() second_round_sorted_prdcts [vector]]]);
    //      end
    //     end block exit_block;
    //   end
    //    for (let k, k < length_of(tar_prdcts_mapping[i].bucket))
    //    begin
    //      for ( let j, j < length_of(contents) )
    //      begin
    //       if(bv_count_one_bits(contents[j].target_prdcts[0]))
    //       begin
    //          if (!(bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].sorted_prdcts_placeholder) or bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].unsorted_prdcts_placeholder)))
    //          begin
    //            tar_prdcts_mapping[i].bucket[k].sorted_prdcts_placeholder = bv_and ( tar_prdcts_mapping[i].bucket[k].products, contents[j].target_prdcts[0] );
    //            tar_prdcts_mapping[i].bucket[k].unsorted_prdcts_placeholder = bv_difference(tar_prdcts_mapping[i].bucket[k].products, tar_prdcts_mapping[i].bucket[k].sorted_prdcts_placeholder);
    //          end
    //          else if ( !(bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_first_round_prdcts) or bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_second_round_prdcts) or bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_first_round_prdcts) or bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_second_round_prdcts) or bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].sorted_prdcts.first_round_prdcts)or bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].sorted_prdcts.second_round_prdcts) or bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.first_round_prdcts) or bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.second_round_prdcts)) )
    //           begin
    //             if ( bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].sorted_prdcts_placeholder) and !(bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].sorted_prdcts.first_round_prdcts) and bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].sorted_prdcts.second_round_prdcts)) and !( length_of(tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_first_round_sorted_prdcts) or length_of( tar_prdcts_mapping[i].bucket[k].sorted_prdcts.first_round_sorted_prdcts) or length_of(tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_second_round_sorted_prdcts) or length_of(tar_prdcts_mapping[i].bucket[k].sorted_prdcts.second_round_sorted_prdcts)))
    //             begin
    //               tar_prdcts_mapping[i].bucket[k].sorted_prdcts.first_round_prdcts = bv_and ( tar_prdcts_mapping[i].bucket[k].sorted_prdcts_placeholder, contents[j].target_prdcts[0] );
    //               tar_prdcts_mapping[i].bucket[k].sorted_prdcts.second_round_prdcts = bv_difference(tar_prdcts_mapping[i].bucket[k].sorted_prdcts_placeholder, tar_prdcts_mapping[i].bucket[k].sorted_prdcts.first_round_prdcts );
    //             end
    //             if ( bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].unsorted_prdcts_placeholder) and !(bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.first_round_prdcts) and bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.second_round_prdcts)) and !( length_of(tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_first_round_sorted_prdcts) or length_of( tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.first_round_sorted_prdcts) or length_of(tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_second_round_sorted_prdcts) or length_of(tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.second_round_sorted_prdcts)))
    //             begin
    //               tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.first_round_prdcts = bv_and ( tar_prdcts_mapping[i].bucket[k].unsorted_prdcts_placeholder, contents[j].target_prdcts[0] );
    //               tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.second_round_prdcts = bv_difference(tar_prdcts_mapping[i].bucket[k].unsorted_prdcts_placeholder, tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.first_round_prdcts );
    //             end
    //           end
    //           else if ( !(bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_first_round_prdcts) or bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_second_round_prdcts) or bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_first_round_prdcts) or bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_second_round_prdcts)))
    //             begin
    //                if ( (bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].sorted_prdcts.first_round_prdcts) or bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].sorted_prdcts.second_round_prdcts)) and !(bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_first_round_prdcts) or bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_second_round_prdcts)) )
    //                begin
    //                  tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_first_round_prdcts = bv_and ( tar_prdcts_mapping[i].bucket[k].sorted_prdcts.first_round_prdcts, contents[j].target_prdcts[0] );
    //                  tar_prdcts_mapping[i].bucket[k].sorted_prdcts.first_round_prdcts = bv_difference(tar_prdcts_mapping[i].bucket[k].sorted_prdcts.first_round_prdcts, tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_first_round_prdcts );
    //                  tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_second_round_prdcts = bv_and ( tar_prdcts_mapping[i].bucket[k].sorted_prdcts.second_round_prdcts, contents[j].target_prdcts[0] );
    //                  tar_prdcts_mapping[i].bucket[k].sorted_prdcts.second_round_prdcts = bv_difference(tar_prdcts_mapping[i].bucket[k].sorted_prdcts.second_round_prdcts, tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_second_round_prdcts);
    //                end
    //                if ( (bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.first_round_prdcts) or bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.second_round_prdcts)) and !(bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_first_round_prdcts) or bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_second_round_prdcts)) )
    //                begin
    //                  tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_first_round_prdcts = bv_and ( tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.first_round_prdcts, contents[j].target_prdcts[0] );
    //                  tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.first_round_prdcts = bv_difference(tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.first_round_prdcts, tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_first_round_prdcts );
    //                  tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_second_round_prdcts = bv_and ( tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.second_round_prdcts, contents[j].target_prdcts[0] );
    //                  tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.second_round_prdcts = bv_difference(tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.second_round_prdcts, tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_second_round_prdcts);
    //                end
    //              end
    //              else
    //              begin
    //                if( bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_first_round_prdcts) )
    //                begin
    //                 _prdcts = bv_and ( tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_first_round_prdcts, contents[j].target_prdcts[0] );
    //                   if(bv_count_one_bits(_prdcts))
    //                   begin
    //                     tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_first_round_sorted_prdcts = vector_append(tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_first_round_sorted_prdcts, _prdcts );
    //                     tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_first_round_prdcts = bv_difference ( tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_first_round_prdcts, _prdcts );
    //                     _prdcts = bv_all_zeros();
    //                   end
    //                end
    //                if( bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].sorted_prdcts.first_round_prdcts) )
    //                begin
    //                 _prdcts = bv_and ( tar_prdcts_mapping[i].bucket[k].sorted_prdcts.first_round_prdcts, contents[j].target_prdcts[0] );
    //                   if(bv_count_one_bits(_prdcts))
    //                   begin
    //                     tar_prdcts_mapping[i].bucket[k].sorted_prdcts.first_round_sorted_prdcts = vector_append (tar_prdcts_mapping[i].bucket[k].sorted_prdcts.first_round_sorted_prdcts, _prdcts );
    //                     tar_prdcts_mapping[i].bucket[k].sorted_prdcts.first_round_prdcts = bv_difference ( tar_prdcts_mapping[i].bucket[k].sorted_prdcts.first_round_prdcts, _prdcts );
    //                     _prdcts = bv_all_zeros();
    //                   end
    //                end
    //                if( bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_second_round_prdcts) )
    //                begin
    //                 _prdcts = bv_and ( tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_second_round_prdcts, contents[j].target_prdcts[0] );
    //                   if(bv_count_one_bits(_prdcts))
    //                   begin
    //                     tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_second_round_sorted_prdcts = vector_append (tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_second_round_sorted_prdcts, _prdcts );
    //                     tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_second_round_prdcts = bv_difference ( tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_second_round_prdcts, _prdcts );
    //                     _prdcts = bv_all_zeros();
    //                   end
    //                end
    //                if( bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].sorted_prdcts.second_round_prdcts) )
    //                begin
    //                 _prdcts = bv_and ( tar_prdcts_mapping[i].bucket[k].sorted_prdcts.second_round_prdcts, contents[j].target_prdcts[0] );
    //                   if(bv_count_one_bits(_prdcts))
    //                   begin
    //                     tar_prdcts_mapping[i].bucket[k].sorted_prdcts.second_round_sorted_prdcts = vector_append (tar_prdcts_mapping[i].bucket[k].sorted_prdcts.second_round_sorted_prdcts, _prdcts );
    //                     tar_prdcts_mapping[i].bucket[k].sorted_prdcts.second_round_prdcts = bv_difference ( tar_prdcts_mapping[i].bucket[k].sorted_prdcts.second_round_prdcts, _prdcts );
    //                     _prdcts = bv_all_zeros();
    //                   end
    //                end
    //                if( bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_first_round_prdcts) )
    //                begin
    //                 _prdcts = bv_and ( tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_first_round_prdcts, contents[j].target_prdcts[0] );
    //                   if(bv_count_one_bits(_prdcts))
    //                   begin
    //                     tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_first_round_sorted_prdcts = vector_append (tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_first_round_sorted_prdcts, _prdcts );
    //                     tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_first_round_prdcts = bv_difference ( tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_first_round_prdcts, _prdcts );
    //                     _prdcts = bv_all_zeros();
    //                   end
    //                end
    //                if( bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.first_round_prdcts) )
    //                begin
    //                 _prdcts = bv_and ( tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.first_round_prdcts, contents[j].target_prdcts[0] );
    //                   if(bv_count_one_bits(_prdcts))
    //                   begin
    //                     tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.first_round_sorted_prdcts = vector_append (tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.first_round_sorted_prdcts, _prdcts );
    //                     tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.first_round_prdcts = bv_difference ( tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.first_round_prdcts, _prdcts );
    //                     _prdcts = bv_all_zeros();
    //                   end
    //                end
    //                if( bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_second_round_prdcts) )
    //                begin
    //                 _prdcts = bv_and ( tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_second_round_prdcts, contents[j].target_prdcts[0] );
    //                   if(bv_count_one_bits(_prdcts))
    //                   begin
    //                     tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_second_round_sorted_prdcts = vector_append (tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_second_round_sorted_prdcts, _prdcts );
    //                     tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_second_round_prdcts = bv_difference ( tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_second_round_prdcts, _prdcts );
    //                     _prdcts = bv_all_zeros();
    //                   end
    //                end
    //                if( bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.second_round_prdcts) )
    //                begin
    //                 _prdcts = bv_and ( tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.second_round_prdcts, contents[j].target_prdcts[0] );
    //                   if(bv_count_one_bits(_prdcts))
    //                   begin
    //                     tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.second_round_sorted_prdcts = vector_append (tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.second_round_sorted_prdcts, _prdcts );
    //                     tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.second_round_prdcts = bv_difference ( tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.second_round_prdcts, _prdcts );
    //                     _prdcts = bv_all_zeros();
    //                   end
    //                end
    //             end
    //            end
    //      end
    //      if ( bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].sorted_prdcts_placeholder))
    //      begin
    //          if( bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_first_round_prdcts) )
    //               tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_first_round_sorted_prdcts = vector_append (tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_first_round_sorted_prdcts, tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_first_round_prdcts );
    //          if( bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].sorted_prdcts.first_round_prdcts) )
    //               tar_prdcts_mapping[i].bucket[k].sorted_prdcts.first_round_sorted_prdcts = vector_append (tar_prdcts_mapping[i].bucket[k].sorted_prdcts.first_round_sorted_prdcts, tar_prdcts_mapping[i].bucket[k].sorted_prdcts.first_round_prdcts );
    //          if( bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_second_round_prdcts) )
    //               tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_second_round_sorted_prdcts = vector_append (tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_second_round_sorted_prdcts, tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_second_round_prdcts );
    //          if( bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].sorted_prdcts.second_round_prdcts) )
    //               tar_prdcts_mapping[i].bucket[k].sorted_prdcts.second_round_sorted_prdcts = vector_append (tar_prdcts_mapping[i].bucket[k].sorted_prdcts.second_round_sorted_prdcts, tar_prdcts_mapping[i].bucket[k].sorted_prdcts.second_round_prdcts );
    //          tar_prdcts_mapping[i].bucket[k].final_sorted_prdcts = vector_concat(tar_prdcts_mapping[i].bucket[k].final_sorted_prdcts,
    //                                                                                     tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_first_round_sorted_prdcts,
    //                                                                                     tar_prdcts_mapping[i].bucket[k].sorted_prdcts.first_round_sorted_prdcts,
    //                                                                                     tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_second_round_sorted_prdcts,
    //                                                                                     tar_prdcts_mapping[i].bucket[k].sorted_prdcts.second_round_sorted_prdcts);
    //         if( !length_of(tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_first_round_sorted_prdcts) and
    //             !length_of(tar_prdcts_mapping[i].bucket[k].sorted_prdcts.first_round_sorted_prdcts) and
    //             !length_of(tar_prdcts_mapping[i].bucket[k].sorted_prdcts.final_second_round_sorted_prdcts) and
    //             !length_of(tar_prdcts_mapping[i].bucket[k].sorted_prdcts.second_round_sorted_prdcts)
    //            )
    //            tar_prdcts_mapping[i].bucket[k].final_sorted_prdcts =vector_append(tar_prdcts_mapping[i].bucket[k].final_sorted_prdcts, tar_prdcts_mapping[i].bucket[k].sorted_prdcts_placeholder);
    //      end
    //      if ( bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].unsorted_prdcts_placeholder))
    //      begin
    //         if( bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_first_round_prdcts) )
    //              tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_first_round_sorted_prdcts = vector_append (tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_first_round_sorted_prdcts, tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_first_round_prdcts );
    //         if( bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.first_round_prdcts) )
    //              tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.first_round_sorted_prdcts = vector_append (tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.first_round_sorted_prdcts, tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.first_round_prdcts );
    //         if( bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_second_round_prdcts) )
    //              tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_second_round_sorted_prdcts = vector_append (tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_second_round_sorted_prdcts, tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_second_round_prdcts );
    //         if( bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.second_round_prdcts) )
    //              tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.second_round_sorted_prdcts = vector_append (tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.second_round_sorted_prdcts, tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.second_round_prdcts );
    //         tar_prdcts_mapping[i].bucket[k].final_sorted_prdcts = vector_concat(tar_prdcts_mapping[i].bucket[k].final_sorted_prdcts,
    //                                                                                     tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_first_round_sorted_prdcts,
    //                                                                                     tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.first_round_sorted_prdcts,
    //                                                                                     tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_second_round_sorted_prdcts,
    //                                                                                     tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.second_round_sorted_prdcts);
    //         if( !length_of(tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_first_round_sorted_prdcts) and
    //             !length_of(tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.first_round_sorted_prdcts) and
    //             !length_of(tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.final_second_round_sorted_prdcts) and
    //             !length_of(tar_prdcts_mapping[i].bucket[k].unsorted_prdcts.second_round_sorted_prdcts)
    //             )
    //         tar_prdcts_mapping[i].bucket[k].final_sorted_prdcts = vector_append(tar_prdcts_mapping[i].bucket[k].final_sorted_prdcts, tar_prdcts_mapping[i].bucket[k].unsorted_prdcts_placeholder);
    //      end
    //      if(!bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].sorted_prdcts_placeholder) and !bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].unsorted_prdcts_placeholder) and bv_count_one_bits(tar_prdcts_mapping[i].bucket[k].products))
    //        tar_prdcts_mapping[i].bucket[k].final_sorted_prdcts = vector_append(tar_prdcts_mapping[i].bucket[k].final_sorted_prdcts, tar_prdcts_mapping[i].bucket[k].products);
    //    end
    //  end
    //  out :: tar_prdcts_mapping;
    // end;
    // out :: gpi14_sorting(dl_bits,gpi14_container,alt_rank,in_constituent_group,in_constituent_reqd,udl_nm) =
    // begin
    //   let int[int] gpi14_matching_dl_bits = allocate();
    //   let int[int] gpi14_non_matching_dl_bits = allocate();
    //   let int lv_dl_bit;
    //   let alt_rank_t[int] alt_rank_vec = allocate();
    //   let len = 0;
    //   let lv_alt_rank = alt_rank;
    //   gpi14_matching_dl_bits     = vector_intersection(dl_bits,gpi14_container);
    //   for(let _dlbit in dl_bits)
    //   begin
    //   if (_dlbit not member gpi14_matching_dl_bits)
    //   gpi14_non_matching_dl_bits= vector_append(gpi14_non_matching_dl_bits,_dlbit);
    //   end
    //   len = length_of(gpi14_matching_dl_bits);
    //   if(len)
    //   begin
    //      alt_rank_vec = vector_append(alt_rank_vec, [record alt_rank lv_alt_rank alt_prdcts gpi14_matching_dl_bits , constituent_group in_constituent_group constituent_reqd in_constituent_reqd udl_nm udl_nm]);
    //      lv_alt_rank = lv_alt_rank + len;
    //   end
    //   len = length_of(gpi14_non_matching_dl_bits);
    //   if(len)
    //   begin
    //      alt_rank_vec = vector_append(alt_rank_vec, [record alt_rank lv_alt_rank alt_prdcts gpi14_non_matching_dl_bits constituent_group in_constituent_group constituent_reqd in_constituent_reqd udl_nm udl_nm]);
    //      lv_alt_rank = lv_alt_rank + len;
    //   end
    //   out :: alt_rank_vec;
    // end;
    // out :: apply_gpi14_sorting(target_dl_bits,bucket_index,udl_index,products,alt_bucket_vec,tac_contents,tar_roa_df_alt,steptal_inp,tar_udl_nm,final_sorted_prdcts_step1) =
    // begin
    // let target_prdct_t[int] unsorted_target_prdct_dtl = allocate();
    // let alt_rank_t[int] alt_rank_vec = allocate();
    // let alt_rank_t[int] lv_alt_rank_vec = allocate();
    // let int[int] alt_dl_bits = allocate();
    // let int[int] lv_target_dl_bits = allocate();
    // let int[int] lv_gpi14_dl_bits = allocate();
    // let int[int] lv_target_dl_bits1 = allocate();
    // let bit_vector_t lv_target_prdcts = bv_all_zeros();
    // let bit_vector_t lv_target_prdcts_sec = bv_all_zeros();
    // let int[int] tac_target_dl_bits = allocate();
    // let int[int] lv_tac_target_dl_bits = allocate();
    // let int tar_idx=0;
    // let int alt_idx=0;
    // let bit_vector_t[int] tar_roa_df_alt_cntnt=allocate();
    // let bit_vector_t[int] tar_roa_df_alt_cntnt1=allocate();
    // let bit_vector_t[int] tar_roa_df_alt_cntnt2=allocate();
    // let decimal('\x01') lv_udl_index = 0;
    // let decimal('\x01') udl_prdct_count = 0;
    // let string(14) _gpi14;
    // let bit_vector_t unmatching_udl_prd=bv_all_zeros();
    // let tac_index = 0;
    // let int[int] target_tac_index = [vector];
    // let bit_vector_t[int] in_tar_vec = [vector];
    // let int[int] tar_bits = allocate();
    // let bit_vector_t[int] final_sorted_prdcts1 = [vector];
    // let bit_vector_t[int] final_sorted_prdcts2 = [vector];
    // let st_len =0;
    // let len_now =0;
    // let len_alt =0;
    // let length_overlapping = 0;
    // let alt_idx1 = 0;
    // let int[int] alt_idx2 = allocate();
    //       for(let dl_bit in target_dl_bits)
    //       begin
    //         let wc=0;
    //         if( dl_bit not member lv_target_dl_bits)
    //         begin
    //            let alt_rank = 1;
    //            let tac_idx = -1;
    //            let idx=-1;
    //            let idx_ord =-1;
    //            let first_hit_priority = 0;
    //            target_tac_index=allocate();
    //            lv_target_prdcts_sec=allocate();
    //            final_sorted_prdcts1 = allocate();
    //            final_sorted_prdcts2 = allocate();
    //            len_now=length_of(alt_bucket_vec);
    //            st_len=length_of(steptal_inp);
    //            length_overlapping=length_of(final_sorted_prdcts_step1);
    //            tar_bits = allocate();
    //            _gpi14 = lookup(prod_dtl, "LKP: Prod",dl_bit).gpi14;
    //            lv_target_prdcts = bv_and(lookup(rule_prod_dtl, "LKP: Rule Products", 'GPI14', 'eq',_gpi14).products,products);
    //            begin block exit_block
    //                for(let rec in tac_contents.contents)
    //                begin
    //                 let lv_target_prdcts1 = bv_and(lv_target_prdcts,rec.target_prdcts);
    //                 target_tac_index=allocate();
    //                 lv_target_prdcts_sec=allocate();
    //                  lv_tac_target_dl_bits = bv_indices(lv_target_prdcts1);
    //                  tac_idx = tac_idx + 1;
    //                  if( is_defined(lv_tac_target_dl_bits) and length_of(lv_tac_target_dl_bits) )
    //                  begin
    //                   target_tac_index = vector_append(target_tac_index,tac_idx);
    //                   first_hit_priority = rec.priority;
    //                   for(let i=tac_idx+1,i<length_of(tac_contents.contents) and first_hit_priority == tac_contents.contents[i].priority)
    //                   begin block my_target_block
    //                    lv_target_prdcts_sec = bv_and(lv_target_prdcts1,tac_contents.contents[i].target_prdcts);
    //                    if(bv_count_one_bits(lv_target_prdcts_sec)) target_tac_index = vector_append(target_tac_index,i);
    //                   end block my_target_block;
    //                    for(let bucket1 in alt_bucket_vec)
    //                    begin
    //                       tar_idx=bucket1.tar_content_index;
    //                       alt_idx=bucket1.alt_index;
    //                       tar_roa_df_alt_cntnt=tar_roa_df_alt[tar_idx].alt_prdcts_all_prio[alt_idx];
    //                       unmatching_udl_prd=allocate();
    //                       if(bv_count_one_bits(bucket1.sorted_prdcts_placeholder) or bv_count_one_bits(bucket1.unsorted_prdcts_placeholder) or bv_count_one_bits(bucket1.products))
    //                       begin
    //                             for( let alt_prds in bucket1.final_sorted_prdcts)
    //                             begin
    //                             unmatching_udl_prd=allocate();
    //                                for(let alt_tac_idx in target_tac_index)
    //                                  begin block my_alt_block
    //                                  unmatching_udl_prd = bv_or(unmatching_udl_prd, bv_and(alt_prds,tac_contents.contents[alt_tac_idx].alt_prdcts));
    //                                  end block my_alt_block;
    //                                  alt_dl_bits =apply_udl_sort(unmatching_udl_prd,bucket1.udl_nm,tar_roa_df_alt_cntnt,st_len,1);
    //                               if(length_of(alt_dl_bits))
    //                               begin
    //                                  lv_alt_rank_vec = if(length_of(alt_dl_bits) > 1) gpi14_sorting(alt_dl_bits,lv_gpi14_dl_bits,alt_rank,bucket1.constituent_group,bucket1.constituent_reqd,bucket1.udl_nm ) else [vector [record alt_rank alt_rank alt_prdcts alt_dl_bits constituent_group bucket1.constituent_group constituent_reqd bucket1.constituent_reqd udl_nm bucket1.udl_nm ]];
    //                                  alt_rank_vec = vector_concat(alt_rank_vec,lv_alt_rank_vec);
    //                                  udl_prdct_count = udl_prdct_count + length_of(alt_dl_bits);
    //                                  alt_rank = if(lv_udl_index != bucket1.udl_index)  udl_prdct_count + 1
    //                                             else alt_rank + length_of(alt_dl_bits);
    //                                  lv_udl_index = bucket1.udl_index;
    //                               end
    //                             end
    //                       end
    //                       len_now=len_alt-1;
    //                    end
    //                    udl_prdct_count = 0;
    //                    if(length_of(alt_rank_vec))
    //                     begin
    //                       tac_index = tac_index + 1;
    //                       for(let p in lv_tac_target_dl_bits)
    //                         unsorted_target_prdct_dtl = vector_append(unsorted_target_prdct_dtl, [record target_dl_bit p bucket_index bucket_index udl_index udl_index tac_index tac_index alt_prdcts_rank alt_rank_vec has_alt 'Y' ST_flag rec.ST_flag]);
    //                       alt_rank_vec = allocate();
    //                       lv_target_dl_bits = vector_concat(lv_target_dl_bits,lv_tac_target_dl_bits);
    //                       lv_target_prdcts = bv_difference(lv_target_prdcts,lv_target_prdcts1);
    //                       if ( !bv_count_one_bits(lv_target_prdcts) )
    //                         exit exit_block;
    //                     end
    //                    end
    //                  end
    //               end block exit_block;
    //             lv_target_dl_bits1 = bv_indices(lv_target_prdcts);
    //            for( let p in lv_target_dl_bits1 )
    //            begin
    //              unsorted_target_prdct_dtl = vector_append(unsorted_target_prdct_dtl, [record target_dl_bit p bucket_index 0 udl_index 0 tac_index 0 alt_prdcts_rank [ vector ] has_alt 'N' ST_flag 0]);
    //            end;
    //            lv_target_dl_bits = vector_concat(lv_target_dl_bits,lv_target_dl_bits1);
    //            lv_target_dl_bits1 = allocate();
    //         end
    //      end
    //     out :: unsorted_target_prdct_dtl;
    // end;
    // out :: apply_udl_sort(unmatching_udl_prd,udl_nm,tar_roa_df_alt_cntnt,st_len,wcx)=
    // begin
    //     let bit_vector_t matching_udl_prd;
    //     let bit_vector_t lv_unmatching_udl_prd;
    //     let bit_vector_t _roa_df_match;
    //     let int[int] lv_alt_dl_bits = allocate();
    //     let bit_vector_t[int] alt_udl_content_lkp  = [vector];
    //     let bit_vector_t[int] roa_df_alt_dtl=tar_roa_df_alt_cntnt;
    //     let record
    //                 int dl_bits;
    //                 string(14)gpi14;
    //         end [int] udl_rule_prd = allocate_with_defaults();
    //     let string("") lv_udl_nm=string_lrtrim(udl_nm);
    //      if((lv_udl_nm=='N/A'))
    //      begin
    //        _roa_df_match   = unmatching_udl_prd;
    //        begin block roa_df_blk_1
    //           for (let k in roa_df_alt_dtl)
    //           begin
    //                 udl_rule_prd=allocate_with_defaults();
    //                 lv_unmatching_udl_prd=bv_and(_roa_df_match,k);
    //                 _roa_df_match =bv_difference(_roa_df_match,lv_unmatching_udl_prd);
    //                 if(bv_count_one_bits(lv_unmatching_udl_prd))
    //                 begin
    //                         for(let p in bv_indices(lv_unmatching_udl_prd))udl_rule_prd = vector_append(udl_rule_prd,[record dl_bits p gpi14 lookup(prod_dtl, "LKP: Prod",p).gpi14]);
    //                         udl_rule_prd=vector_sort(udl_rule_prd,{gpi14});
    //                         lv_alt_dl_bits=vector_concat(lv_alt_dl_bits,for(let i in udl_rule_prd):i.dl_bits);
    //                 end
    //                 if(!bv_count_one_bits(_roa_df_match))
    //                         exit roa_df_blk_1;
    //          end
    //          end block roa_df_blk_1;
    //     end
    //     else
    //     begin
    //         alt_udl_content_lkp   = first_without_error( first_defined(lookup(cag_udl_lkp_wt_rule_priority, "Expanded_UDL_wt_rl_priority",lv_udl_nm).contents, lookup(baseline_udl_lkp_wt_rule_priority,"Expanded_UDL_wt_rl_priority",lv_udl_nm).contents,[vector]) , [vector] );
    //         _roa_df_match   = unmatching_udl_prd;
    //         begin block roa_df_blk
    //           for (let k in roa_df_alt_dtl)
    //           begin
    //                 lv_unmatching_udl_prd=bv_and(_roa_df_match,k);
    //                 _roa_df_match =bv_difference(_roa_df_match,lv_unmatching_udl_prd);
    //                 begin block udl_blk
    //                         for(let c in alt_udl_content_lkp)
    //                         begin
    //                                 udl_rule_prd= allocate_with_defaults();
    //                                 matching_udl_prd=bv_and(lv_unmatching_udl_prd,c);
    //                                 lv_unmatching_udl_prd=bv_difference(lv_unmatching_udl_prd,matching_udl_prd);
    //                                 if(bv_count_one_bits(matching_udl_prd))
    //                                 begin
    //                                         for(let p in bv_indices(matching_udl_prd))udl_rule_prd = vector_append(udl_rule_prd,[record dl_bits p gpi14 lookup(prod_dtl, "LKP: Prod",p).gpi14]);
    //                                         udl_rule_prd=vector_sort(udl_rule_prd,{gpi14});
    //                                         lv_alt_dl_bits=vector_concat(lv_alt_dl_bits,for(let i in udl_rule_prd):i.dl_bits);
    //                                 end
    //                                 if(!bv_count_one_bits(lv_unmatching_udl_prd))
    //                                         exit udl_blk;
    //                         end
    //                 end block udl_blk;
    //                 if(!bv_count_one_bits(_roa_df_match))
    //                         exit roa_df_blk;
    //          end
    //          end block roa_df_blk;
    //     end
    // out :: lv_alt_dl_bits;
    // end;
    //
    val out = in
    out
  }

}
