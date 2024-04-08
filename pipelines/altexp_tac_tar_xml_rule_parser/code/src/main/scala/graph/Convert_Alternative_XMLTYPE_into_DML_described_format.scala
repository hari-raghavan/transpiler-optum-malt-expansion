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

object Convert_Alternative_XMLTYPE_into_DML_described_format {
  def apply(context: Context, in: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    //
    // @TODO: Unknown Component xml-split
    //
    // <component>
    //       <name>Convert_Alternative_XMLTYPE_into_DML_described_format</name>
    //       <type>xml-split</type>
    //       <layout>layout-Convert_Alternative_XMLTYPE_into_DML_described_format</layout>
    //       <in-port><format>type qual_priority_t =
    // record
    //   string(int) qual;
    //   decimal(&quot;&quot;) priority;
    // end;
    // constant string(int)[int] OVERRIDE_QUALIFIERS = [vector 'DESI_CD', 'DOSAGE_FORM', 'MSC', 'ROA', 'RXOTC' ];
    // constant qual_priority_t[int] QUAL_PRIORITY =[vector [record  qual &quot;NDC11&quot;        priority &quot;1&quot;],
    //                                                       [record qual &quot;NDC9&quot;         priority &quot;2&quot;],
    //                                                       [record qual &quot;NDC5&quot;         priority &quot;3&quot;],
    //                                                       [record qual &quot;GPI14&quot;        priority &quot;4&quot;],
    //                                                       [record qual &quot;GPI12&quot;        priority &quot;5&quot;],
    //                                                       [record qual &quot;GPI10&quot;        priority &quot;6&quot;],
    //                                                       [record qual &quot;DOSAGE_FORM&quot;  priority &quot;7&quot;],
    //                                                       [record qual &quot;ROA&quot;          priority &quot;8&quot;],
    //                                                       [record qual &quot;DRUG_NAME&quot;    priority &quot;9&quot;],
    //                                                       [record qual &quot;GPI8&quot;         priority &quot;10&quot;],
    //                                                       [record qual &quot;GPI6&quot;         priority &quot;11&quot;],
    //                                                       [record qual &quot;GPI4&quot;         priority &quot;12&quot;],
    //                                                       [record qual &quot;MSC&quot;          priority &quot;13&quot;],
    //                                                       [record qual &quot;RXOTC&quot;        priority &quot;14&quot;],
    //                                                       [record qual &quot;DAYS_UNTIL_DRUG_STATUS_INACTIVE&quot; priority &quot;15&quot;] ,
    //                                                       [record qual &quot;STATUS_CD&quot;  priority   &quot;16&quot;],
    //                                                       [record qual &quot;REPACKAGER&quot;   priority &quot;17&quot;],
    //                                                       [record qual &quot;DESI_CD&quot;    priority   &quot;18&quot;]];
    // type drug_data_set_dtl_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20) rxclaim_env_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) group = NULL(&quot;&quot;) ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) run_eff_dt = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) drug_data_set_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) drug_data_set_id ;
    //   string(&quot;\x01&quot;, maximum_length=11) ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=14) gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=1) status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=8) eff_dt = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=8) term_dt = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=8) inactive_dt = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=1) msc ;
    //   string(&quot;\x01&quot;, maximum_length=70) drug_name ;
    //   string(&quot;\x01&quot;, maximum_length=30) prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=3) rx_otc ;
    //   string(&quot;\x01&quot;, maximum_length=1) rx_otc_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1) desi ;
    //   string(&quot;\x01&quot;, maximum_length=2) roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4) dosage_form_cd ;
    //   decimal(&quot;\x01&quot;.5, maximum_length=15) prod_strength ;
    //   string(&quot;\x01&quot;, maximum_length=1) repack_cd ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi8_desc ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type user_defined_list_rule_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) user_defined_list_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) user_defined_list_name ;
    //   string(&quot;\x01&quot;, maximum_length=60) user_defined_list_desc ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) user_defined_list_rule_id ;
    //   string(&quot;\x01&quot;, maximum_length=4000) rule = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=1) incl_cd ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type rule_xwalk_t=
    // record
    //   decimal(&quot;\x01&quot;) udl_id;
    //   decimal(&quot;\x01&quot;) udl_rule_id;
    //   string(&quot;\x01&quot;) udl_nm;
    //   string(&quot;\x01&quot;) udl_desc;
    //   decimal(&quot;\x01&quot;) rule_priority;
    //   string(1) inclusion_cd;
    //   string(int) qualifier_cd;
    //   string(int) operator;
    //   string(int) compare_value;
    //   string(1) conjunction_cd;
    //   decimal(&quot;\x01&quot;) rule_expression_id;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type udl_mstr_xwalk_t=
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) udl_id;
    //   string(&quot;\x01&quot;, maximum_length=20) udl_nm;
    //   string(&quot;\x01&quot;, maximum_length=60) user_defined_list_desc;
    //   string(int)[int] qual_list;
    //   decimal(1) override_flg;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type product_lkp_t =
    // record
    //   int dl_bit;
    //   string(&quot;\x01&quot;, maximum_length=11)  ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=14)  gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=1)  status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=8) inactive_dt ;
    //   string(&quot;\x01&quot;, maximum_length=1)  msc ;
    //   string(&quot;\x01&quot;, maximum_length=70)  drug_name ;
    //   string(&quot;\x01&quot;, maximum_length=3)  rx_otc ;
    //   string(&quot;\x01&quot;, maximum_length=1)  desi ;
    //   string(&quot;\x01&quot;, maximum_length=2)  roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4)  dosage_form_cd ;
    //   decimal(&quot;\x01&quot;.5, maximum_length=15) prod_strength ;
    //   string(&quot;\x01&quot;, maximum_length=1)  repack_cd ;
    //   string(&quot;\x01&quot;, maximum_length=30) prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi8_desc ;
    //   string(1) newline = &quot;\n&quot;;
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
    //   string(&quot;\x01&quot;, maximum_length=20)  carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20)  account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20)  group = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=11)  ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=14)  gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=1)  status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=8) eff_dt = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=8) term_dt = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=8) inactive_dt ;
    //   string(&quot;\x01&quot;, maximum_length=1)  msc ;
    //   string(&quot;\x01&quot;, maximum_length=70)  drug_name ;
    //   string(&quot;\x01&quot;, maximum_length=3)  rx_otc ;
    //   string(&quot;\x01&quot;, maximum_length=1)  desi ;
    //   string(&quot;\x01&quot;, maximum_length=2)  roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4)  dosage_form_cd ;
    //   decimal(&quot;\x01&quot;.5, maximum_length=15) prod_strength ;
    //   string(&quot;\x01&quot;, maximum_length=1)  repack_cd ;
    //   string(&quot;\x01&quot;, maximum_length=30) prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi8_desc ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type cag_ovrrd_ref_file_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20)  carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20)  account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20)  group = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=1) is_future_snap = 0;
    //   string(&quot;\x01&quot;) data_path;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type udl_exp_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) udl_id;
    //   string(&quot;\x01&quot;, maximum_length=20) udl_nm;
    //   string(&quot;\x01&quot;, maximum_length=60) udl_desc;
    //   bit_vector_t products;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
    //   bit_vector_t[int] contents;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tal_dtl_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_name ;
    //   string(&quot;\x01&quot;, maximum_length=60) tal_desc ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tal_dtl_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=20) nested_tal_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;, 6, maximum_length=39) priority ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tal_assoc_dtl_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name ;
    //   string(&quot;\x01&quot;, maximum_length=60) tal_assoc_desc ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tal_assoc_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=20) target_udl_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) alt_udl_name = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) alt_rank = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) constituent_rank = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=2) constituent_group = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=1) constituent_reqd = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=30) shared_qual ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tac_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tar_name = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tac_dtl_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tac_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tac_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tac_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) priority ;
    //   string(&quot;\x01&quot;, maximum_length=4000) target_rule = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=4000) alt_rule = NULL(&quot;&quot;) ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tac_rule_xwalk_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tac_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tac_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) priority ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
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
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type contents_alt_st =
    // record
    //  bit_vector_t target_prdcts;
    //  bit_vector_t alternate_prdcts;
    //  string(&quot;\x01&quot;, maximum_length=40) tal_assoc_name;
    // end;
    // type tac_st_contents =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_id;
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
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tac_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tac_name ;
    //   tac_contents_t[int] tac_contents;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
    //   string(1) newline = '\n';
    // end;
    // type tal_container_xwalk_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20) tal_name ;
    //   int target_dl_bit;
    //   record
    //    int alt_prd;
    //    int alt_rank;
    //   end[int] alt_prdcts;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type target_udl_products_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20) target_udl_name = NULL(&quot;&quot;) ;
    //   bit_vector_t target_products;
    // end;
    // type alt_udl_products_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20) alt_udl_name = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) alt_rank = NULL(&quot;&quot;) ;
    //   bit_vector_t alt_products;
    // end;
    // type alt_exclusion_products_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tal_assoc_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tac_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tar_name = NULL(&quot;&quot;) ;
    //   bit_vector_t target_product_dtl;
    //   bit_vector_t alt_product_dtl;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tal_assoc_xwalk_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name ;
    //   string(&quot;\x01&quot;, maximum_length=30) shared_qual ;
    //   string(&quot;\x01&quot;, maximum_length=10) override_tac_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=10) override_tar_name = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tal_assoc_type_cd ;
    //   target_udl_products_t[int] target_udl_products;
    //   alt_udl_products_t[int] alt_udl_products;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type form_data_set_dtl_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) formulary_data_set_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_id = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=10) formulary_cd = &quot;&quot; ;
    //   string(&quot;\x01&quot;, maximum_length=20) carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) group = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) rxclaim_env_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) customer_name = NULL(&quot;&quot;);
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) last_exp_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) run_eff_dt = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) formulary_data_set_dtl_id ;
    //   string(&quot;\x01&quot;, maximum_length=11) ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=2) formulary_tier ;
    //   string(&quot;\x01&quot;, maximum_length=2) formulary_status ;
    //   string(&quot;\x01&quot;, maximum_length=1) pa_reqd_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) specialty_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) step_therapy_ind ;
    //   string(&quot;\x01&quot;, maximum_length=50) formulary_tier_desc ;
    //   string(&quot;\x01&quot;, maximum_length=50) formulary_status_desc ;
    //   string(&quot;\x01&quot;, maximum_length=1) pa_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1) step_therapy_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=100) step_therapy_group_name = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) step_therapy_step_number = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type form_ovrrd_xwalk_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_name ;
    //   string(&quot;\x01&quot;, maximum_length=10) formulary_cd = &quot;&quot; ;
    //   string(&quot;\x01&quot;, maximum_length=20)  carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20)  account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20)  group = NULL(&quot;&quot;) ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) last_exp_dt ;
    //   string(&quot;\x01&quot;, maximum_length=11) ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=2) formulary_tier ;
    //   string(&quot;\x01&quot;, maximum_length=2) formulary_status ;
    //   string(&quot;\x01&quot;, maximum_length=1) pa_reqd_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) specialty_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) step_therapy_ind ;
    //   string(&quot;\x01&quot;, maximum_length=50) formulary_tier_desc ;
    //   string(&quot;\x01&quot;, maximum_length=50) formulary_status_desc ;
    //   string(&quot;\x01&quot;, maximum_length=1) pa_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1) step_therapy_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=100) step_therapy_group_name = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) step_therapy_step_number = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type form_ovrrd_ref_file_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_name ;
    //   string(&quot;\x01&quot;, maximum_length=20)  carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20)  account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20)  group = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) customer_name = NULL(&quot;&quot;);
    //   decimal(&quot;\x01&quot;,0, maximum_length=1) is_future_snap = 0;
    //   string(&quot;\x01&quot;) data_path;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) run_eff_dt = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type output_profile_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) output_profile_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) rxclaim_env_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_id = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) output_profile_form_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) output_profile_job_dtl_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) output_profile_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) alias_name = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) alias_priority = NULL ;
    //   string(&quot;\x01&quot;, maximum_length=20) carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) group = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) tac_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) tar_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) tsd_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) job_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) job_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) customer_name = NULL(&quot;&quot;);
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) run_day = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) lob_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=4) run_jan1_start_mmdd = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=4) run_jan1_end_mmdd = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;) future_flg = 0;
    //   string(&quot;\x01&quot;, maximum_length=30) formulary_pseudonym = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) notes_id = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=60) output_profile_desc ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39 ) formulary_option_cd ;
    //   string(&quot;\x01&quot;, maximum_length=20) layout_name ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) as_of_dt = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=1) st_tac_ind ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type form_product_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=10) formulary_cd = &quot;&quot; ;
    //   string(&quot;\x01&quot;, maximum_length=11) ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=2) formulary_tier ;
    //   string(&quot;\x01&quot;, maximum_length=2) formulary_status ;
    //   string(&quot;\x01&quot;, maximum_length=1) pa_reqd_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) specialty_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) step_therapy_ind ;
    //   string(&quot;\x01&quot;, maximum_length=50) formulary_tier_desc ;
    //   string(&quot;\x01&quot;, maximum_length=50) formulary_status_desc ;
    //   string(&quot;\x01&quot;, maximum_length=1) pa_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1) step_therapy_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=100) step_therapy_group_name = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) step_therapy_step_number = NULL(&quot;&quot;) ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) last_exp_dt ;
    // end;
    // type cag_product_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=11) ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=14) gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=1) status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=8) eff_dt = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=8) term_dt = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=8) inactive_dt ;
    //   string(&quot;\x01&quot;, maximum_length=1) msc ;
    //   string(&quot;\x01&quot;, maximum_length=70) drug_name ;
    //   string(&quot;\x01&quot;, maximum_length=3) rx_otc ;
    //   string(&quot;\x01&quot;, maximum_length=1) desi ;
    //   string(&quot;\x01&quot;, maximum_length=2) roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4) dosage_form_cd ;
    //   decimal(&quot;\x01&quot;.5, maximum_length=15) prod_strength ;
    //   string(&quot;\x01&quot;, maximum_length=1) repack_cd ;
    //   string(&quot;\x01&quot;, maximum_length=30) prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi8_desc ;
    // end;
    // type cag_rlp_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20) carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) group = NULL(&quot;&quot;) ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) run_eff_dt = NULL(&quot;&quot;) ;
    //   decimal(1) cag_priority = 0;
    //   cag_product_t[int] prdcts;
    // end;
    // type form_rlp_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) group = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) customer_name = NULL(&quot;&quot;);
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) run_eff_dt = NULL(&quot;&quot;) ;
    //   decimal(1) cag_priority = 0;
    //   form_product_t[int] prdcts;
    // end;
    // type tsd_dtl_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tsd_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tsd_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tsd_name ;
    //   string(&quot;\x01&quot;, maximum_length=30) tsd_cd ;
    //   string(&quot;\x01&quot;, maximum_length=2) formulary_tier ;
    //   string(&quot;\x01&quot;, maximum_length=2) formulary_status ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) priority ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tsd_xwalk_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tsd_id ;
    //   string(&quot;\x01&quot;, maximum_length=30) tsd_cd ;
    //   bit_vector_t products;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type alt_constituent_prdct_t =
    // record
    //   bit_vector_t alt_prdcts;
    //   string(2) constituent_group = NULL(&quot;&quot;);
    //   string(1) constituent_reqd = NULL(&quot;&quot;);
    //   string(20) udl_nm = NULL(&quot;&quot;);
    // end;
    // type tal_container_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) tar_udl_nm = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=60) tal_desc ;
    //   decimal(&quot;\x01&quot;, 6, maximum_length=39) priority ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tal_assoc_type_cd ;
    //   bit_vector_t[int] target_prdcts;
    //   alt_constituent_prdct_t[int] alt_constituent_prdcts;
    //   string(&quot;\x01&quot;, maximum_length=30) shared_qual ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tac_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tar_name = NULL(&quot;&quot;) ;
    //   string(2)[int] constituent_grp_vec;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type master_cag_mapping_t =
    // record
    //   string(&quot;\x01&quot;) carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;) account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;) group = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;) future_flg = 'C';
    //   string(&quot;\x01&quot;) cag_override_data_path= NULL(&quot;&quot;);
    //   decimal(&quot;\x01&quot;)[int] qual_output_profile_ids ;
    //   record
    //     string(&quot;\x01&quot;) qual_output_profile_name ;
    //     string(&quot;\x01&quot;) rxclaim_env_name;
    //     decimal(&quot;\x01&quot;) [int] job_ids;
    //     string(&quot;\x01&quot;) [int] alias_names;
    //     string(&quot;\x01&quot;) [int] job_names;
    //     string(&quot;\x01&quot;) [int] formulary_names ;
    //     string(&quot;\x01&quot;) [int] form_override_data_paths;
    //     string(&quot;\x01&quot;, maximum_length=30)[int] formulary_pseudonyms;
    //     string(&quot;\x01&quot;, maximum_length=20) tal_name ;
    //     string(&quot;\x01&quot;) tac_name ;
    //     string(&quot;\x01&quot;) tar_name ;
    //     string(&quot;\x01&quot;) tsd_name ;
    //     string(&quot;\x01&quot;, maximum_length=1) st_tac_ind ;
    //   end[int] op_dtls;
    //   record
    //     decimal(&quot;\x01&quot;) non_qual_op_id;
    //     string(&quot;\x01&quot;) rxclaim_env_name;
    //     decimal(&quot;\x01&quot;) [int] job_ids;
    //     string(&quot;\x01&quot;)[int] formulary_names;
    //     string(&quot;\x01&quot;, maximum_length=30)[int] formulary_pseudonyms;
    //     string(&quot;\x01&quot;) [int] alias_names;
    //     string(&quot;\x01&quot;) [int] job_names;
    //   end[int] non_qual_output_profile_ids ;
    //   string(&quot;\x01&quot;) [int] err_msgs;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) as_of_dt = NULL(&quot;&quot;);
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tar_dtl_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tar_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tar_dtl_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tar_name ;
    //   string(&quot;\x01&quot;, maximum_length=1) sort_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) filter_ind ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) priority ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tar_dtl_type_cd ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tar_roa_df_set_id = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=4000) target_rule = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=4000) alt_rule = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=15) rebate_elig_cd = NULL(&quot;&quot;) ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tar_roa_df_set_dtl_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tar_roa_df_set_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tar_roa_df_set_id ;
    //   string(&quot;\x01&quot;, maximum_length=2) target_roa_cd = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=4) target_dosage_form_cd = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=2) alt_roa_cd = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=4) alt_dosage_form_cd = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) priority ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tar_rule_xwalk_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tar_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tar_dtl_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tar_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tar_roa_df_set_id = NULL(&quot;&quot;);
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tar_dtl_type_cd ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) priority ;
    //   string(&quot;\x01&quot;, maximum_length=1) sort_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) filter_ind ;
    //   string(&quot;\x01&quot;, maximum_length=15) rebate_elig_cd = NULL(&quot;&quot;) ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
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
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type alt_run_alt_dtl_load_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=16 ) alt_run_alt_dtl_id = -1 ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=16 ) alt_run_id = -1;
    //   decimal(&quot;\x01&quot;,0, maximum_length=16 ) alt_run_target_dtl_id = -1 ;
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_name ;
    //   string(&quot;\x01&quot;, maximum_length=11) target_ndc ;
    //   string(&quot;\x01&quot;, maximum_length=11) alt_ndc ;
    //   string(&quot;\x01&quot;, maximum_length=2) alt_formulary_tier ;
    //   string(&quot;\x01&quot;, maximum_length=1) alt_multi_src_cd ;
    //   string(&quot;\x01&quot;, maximum_length=2) alt_roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4) alt_dosage_form_cd ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39 ) rank ;
    //   string(&quot;\x01&quot;, maximum_length=1) alt_step_therapy_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) alt_pa_reqd_ind ;
    //   string(&quot;\x01&quot;, maximum_length=2) alt_formulary_status ;
    //   string(&quot;\x01&quot;, maximum_length=1) alt_specialty_ind ;
    //   string(&quot;\x01&quot;, maximum_length=14) alt_gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=70) alt_prod_name_ext ;
    //   string(&quot;\x01&quot;, maximum_length=30) alt_prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=30) alt_prod_short_desc_grp;
    //   string(&quot;\x01&quot;, maximum_length=60) alt_gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) alt_gpi8_desc ;
    //   string(&quot;\x01&quot;, maximum_length=50) alt_formulary_tier_desc ;
    //   string(&quot;\x01&quot;, maximum_length=50) alt_formulary_status_desc ;
    //   string(&quot;\x01&quot;, maximum_length=1) alt_pa_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1) alt_step_therapy_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1000) alt_step_therapy_group_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=100) alt_step_therapy_step_number = NULL(&quot;&quot;) ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) rec_crt_ts ;
    //   string(&quot;\x01&quot;, maximum_length=30) rec_crt_user_id ;
    //   string(&quot;\x01&quot;, maximum_length=15) rebate_elig_cd = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=15) tad_eligible_cd= NULL(&quot;&quot;);
    //   decimal(&quot;\x01&quot;.3, maximum_length=7) alt_qty_adj = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=40) tal_assoc_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=40) tala = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) alt_udl = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;.6, maximum_length=39) tal_assoc_rank ;
    //   string(&quot;\x01&quot;, maximum_length=2) constituent_group = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=1) constituent_reqd = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type alt_run_target_dtl_load_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=16 ) alt_run_target_dtl_id = -1 ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=16 ) alt_run_id  = -1 ;
    //   string(&quot;\x01&quot;, maximum_length=40) tal_assoc_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=40) tala = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) tar_udl = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_name ;
    //   string(&quot;\x01&quot;, maximum_length=11) target_ndc ;
    //   string(&quot;\x01&quot;, maximum_length=2) target_formulary_tier ;
    //   string(&quot;\x01&quot;, maximum_length=2) target_formulary_status ;
    //   string(&quot;\x01&quot;, maximum_length=1) target_pa_reqd_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) target_step_therapy_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) target_specialty_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) target_multi_src_cd ;
    //   string(&quot;\x01&quot;, maximum_length=2) target_roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4) target_dosage_form_cd ;
    //   string(&quot;\x01&quot;, maximum_length=14) target_gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=70) target_prod_name_ext ;
    //   string(&quot;\x01&quot;, maximum_length=30) target_prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) target_gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) target_gpi8_desc ;
    //   string(&quot;\x01&quot;, maximum_length=50) target_formulary_tier_desc ;
    //   string(&quot;\x01&quot;, maximum_length=50) target_formulary_status_desc ;
    //   string(&quot;\x01&quot;, maximum_length=1) target_pa_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1) target_step_therapy_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1000) target_step_therapy_group_name = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) target_step_therapy_step_num = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=10) formulary_cd = NULL(&quot;&quot;) ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) rec_crt_ts ;
    //   string(&quot;\x01&quot;, maximum_length=30) rec_crt_user_id ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) last_exp_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type alt_run_load_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=16) alt_run_id = -1 ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) output_profile_id ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) run_start_ts = NULL(&quot;&quot;) ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) run_complete_ts = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) alt_run_status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1) published_ind ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) rec_crt_ts ;
    //   string(&quot;\x01&quot;, maximum_length=30) rec_crt_user_id ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) rec_last_upd_ts ;
    //   string(&quot;\x01&quot;, maximum_length=30) rec_last_upd_user_id ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) run_eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) as_of_dt = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type alt_run_load_adhoc_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=16) alt_run_id = -1 ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) output_profile_id ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) run_start_ts = NULL(&quot;&quot;) ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) run_complete_ts = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) alt_run_status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1) published_ind ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) rec_crt_ts ;
    //   string(&quot;\x01&quot;, maximum_length=30) rec_crt_user_id ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) rec_last_upd_ts ;
    //   string(&quot;\x01&quot;, maximum_length=30) rec_last_upd_user_id ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) run_eff_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type alt_run_t =
    // record
    //   decimal(&quot;|&quot;) alt_run_id ;
    //   decimal(&quot;|&quot;) output_profile_id ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;|&quot;) run_start_ts = NULL(&quot;&quot;) ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;|&quot;) run_complete_ts = NULL(&quot;&quot;) ;
    //   decimal(&quot;|&quot;) alt_run_status_cd ;
    //   string(&quot;|&quot;) published_ind ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;|&quot;) rec_crt_ts ;
    //   string(&quot;|&quot;) rec_crt_user_id ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;|&quot;) rec_last_upd_ts ;
    //   string(&quot;|&quot;) rec_last_upd_user_id ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\n&quot;) run_eff_dt ;
    // end;
    // type alt_run_alt_proxy_dtl_load_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=16 ) alt_run_id = -1;
    //   decimal(&quot;\x01&quot;,0, maximum_length=16 ) alt_run_alt_dtl_id = -1 ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=16 ) alt_run_target_dtl_id = -1 ;
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_name ;
    //   string(&quot;\x01&quot;, maximum_length=11) target_ndc ;
    //   string(&quot;\x01&quot;, maximum_length=40) tal_assoc_name ;
    //   string(&quot;\x01&quot;, maximum_length=11) alt_proxy_ndc ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39 ) rank ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) rec_crt_ts ;
    //   string(&quot;\x01&quot;, maximum_length=30) rec_crt_user_id ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type file_load_cntl_load_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=50) user_id ;
    //   string(&quot;\x01&quot;, maximum_length=1) file_load_type_cd ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) component_type_cd = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) file_load_status_cd ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) file_load_start_ts = NULL(&quot;&quot;) ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) rec_crt_ts ;
    //   string(&quot;\x01&quot;, maximum_length=30) rec_crt_user_id ;
    //   string(&quot;\x01&quot;, maximum_length=1) published_ind ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=16) alt_run_id = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type file_load_ctl_dtl_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=16) file_load_cntl_id ;
    //   string(&quot;\x01&quot;, maximum_length=4000) component_ids = NULL(&quot;&quot;) ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) as_of_date ;
    //   string(&quot;\x01&quot;, maximum_length=20) rxclaim_env_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) group = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) component_type_cd = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=512) file_name_w = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=1) published_ind ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=16) alt_run_id = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=512) report_file_name = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type file_load_err_dtl_load_t =
    // record
    //   decimal(&quot;,&quot;,0, maximum_length=16) file_load_cntl_id ;
    //   string(&quot;,&quot;, maximum_length=1024) err_desc ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;,&quot;) rec_crt_ts ;
    //   string(&quot;,&quot;, maximum_length=30) rec_crt_user_id ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;,&quot;) rec_last_upd_ts ;
    //   string(&quot;\n&quot;, maximum_length=30) rec_last_upd_user_id ;
    // end;
    // type tal_assoc_prdcts_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tal_assoc_type_cd ;
    //   bit_vector_t[int] target_prdcts;
    //   bit_vector_t[int] alt_prdcts;
    //   string(&quot;\x01&quot;, maximum_length=30) shared_qual ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tac_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tar_name = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type part_exp_master_file_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=16) file_load_cntl_id ;
    //   string(&quot;\x01&quot;, maximum_length=1024) component_ids ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) as_of_date ;
    //   string(&quot;\x01&quot;, maximum_length=20) rxclaim_env_name = &quot;&quot; ;
    //   string(&quot;\x01&quot;, maximum_length=50) cag_in = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=50) cag_out = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39, sign_reserved) component_type_cd = NULL(&quot;&quot;) ;
    //   utf8 string(&quot;\x01&quot;, maximum_length=512) file_name_w = NULL(&quot;&quot;) ;
    //   utf8 string(&quot;\x01&quot;, maximum_length=1) published_ind ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type alt_rank_t =
    // record
    //   int alt_rank;
    //   int[int] alt_prdcts;
    //   string(2) constituent_group = NULL(&quot;&quot;);
    //   string(1) constituent_reqd = NULL(&quot;&quot;);
    //   string(20) udl_nm = NULL(&quot;&quot;);
    // end;
    // type rebate_elig_contents_t =
    // record
    //   string(&quot;\x01&quot;) rebate_elig_cd;
    //   int[int] rebate_elig_prdcts;
    // end;
    // type target_prdct_t =
    // record
    //   int target_dl_bit;
    //   decimal('\x01') bucket_index = 0;
    //   decimal('\x01') udl_index = 0;
    //   decimal('\x01') tac_index = 0;
    //   alt_rank_t[int] alt_prdcts_rank;
    //   string(&quot;\x01&quot;)has_alt;
    //   decimal(1) ST_flag = 0;
    // end;
    // type tal_assoc_adhoc_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name ;
    //   string(&quot;\x01&quot;, maximum_length=2000) clinical_indn_desc = NULL(&quot;&quot;);
    //   string(&quot;\x01&quot;, maximum_length=60) tal_assoc_desc ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tal_assoc_type_cd ;
    //   record
    //     string(&quot;\x01&quot;, maximum_length=20) udl_nm;
    //     string(&quot;\x01&quot;, maximum_length=60) udl_desc;
    //     bit_vector_t products;
    //   end[int] target_udl_info ;
    //   record
    //      string(&quot;\x01&quot;, maximum_length=20) udl_nm;
    //      string(&quot;\x01&quot;, maximum_length=60) udl_desc;
    //      bit_vector_t products;
    //      string(2) constituent_group = NULL(&quot;&quot;);
    //      string(1) constituent_reqd = NULL(&quot;&quot;);
    //      decimal(&quot;\x01&quot;,0, maximum_length=39) constituent_rank = NULL(&quot;&quot;);
    //   end[int] alt_udl_info;
    //   string(&quot;\x01&quot;, maximum_length=30) shared_qual ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tac_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tar_name = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tal_container_adhoc_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=2000) clinical_indn_desc = NULL(&quot;&quot;);
    //   string(&quot;\x01&quot;, maximum_length=60) tal_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) tal_assoc_desc ;
    //   decimal(&quot;\x01&quot;, 6, maximum_length=39) priority ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tal_assoc_type_cd ;
    //   record
    //       string(&quot;\x01&quot;, maximum_length=20) udl_nm;
    //       string(&quot;\x01&quot;, maximum_length=60) udl_desc;
    //       bit_vector_t products;
    //   end[int] target_prdcts;
    //   record
    //       string(&quot;\x01&quot;, maximum_length=20) udl_nm;
    //       string(&quot;\x01&quot;, maximum_length=60) udl_desc;
    //       bit_vector_t products;
    //       string(2) constituent_group = NULL(&quot;&quot;);
    //       string(1) constituent_reqd = NULL(&quot;&quot;);
    //       decimal(&quot;\x01&quot;,0, maximum_length=39) constituent_rank = NULL(&quot;&quot;);
    //   end[int] alt_prdcts;
    //   string(&quot;\x01&quot;, maximum_length=30) shared_qual ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tac_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tar_name = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tal_assoc_adhoc_enrich_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name ;
    //   string(&quot;\x01&quot;, maximum_length=2000) clinical_indn_desc = NULL(&quot;&quot;);
    //   string(&quot;\x01&quot;, maximum_length=60) tal_assoc_desc ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tal_assoc_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=20) udl_id = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=60) udl_desc;
    //   string(&quot;\x01&quot;, maximum_length=20) Target_Alternative;
    //   bit_vector_t products;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) alt_rank = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=30) shared_qual ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tac_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tar_name = NULL(&quot;&quot;) ;
    //   string(2) constituent_group = NULL(&quot;&quot;);
    //   string(1) constituent_reqd = NULL(&quot;&quot;);
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) constituent_rank = NULL(&quot;&quot;);
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tal_container_adhoc_enrich_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20) tal_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_id = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=2000) clinical_indn_desc = NULL(&quot;&quot;);
    //   string(&quot;\x01&quot;, maximum_length=60) tal_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) tal_assoc_desc ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tal_assoc_type_cd ;
    //   decimal(&quot;\x01&quot;, 6, maximum_length=39) tal_assoc_rank = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) udl_id = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=60) udl_desc;
    //   string(&quot;\x01&quot;, maximum_length=20) Target_Alternative;
    //   bit_vector_t products;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) alt_rank = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=30) shared_qual ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tac_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tar_name = NULL(&quot;&quot;) ;
    //   string(2) constituent_group = NULL(&quot;&quot;);
    //   string(1) constituent_reqd = NULL(&quot;&quot;);
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) constituent_rank = NULL(&quot;&quot;);
    //   string(1) newline = &quot;\n&quot;;
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
    //   decimal(&quot;\x01&quot;,0, maximum_length=16 ) alt_run_target_dtl_id ;
    //   string(&quot;\x01&quot;, maximum_length=40) tal_assoc_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_name ;
    //   string(&quot;\x01&quot;, maximum_length=11) target_ndc ;
    // end;
    // type alt_run_alt_dtl_ref_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=16 ) alt_run_alt_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=16 ) alt_run_target_dtl_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_name ;
    //   string(&quot;\x01&quot;, maximum_length=40) tal_assoc_name ;
    //   string(&quot;\x01&quot;, maximum_length=11) target_ndc ;
    //   string(&quot;\x01&quot;, maximum_length=11) alt_ndc ;
    // end;
    // type master_cag_mapping_adhoc_t =
    // record
    //   string(&quot;\x01&quot;) carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;) account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;) group = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;) cag_override_data_path= NULL(&quot;&quot;);
    //   decimal(&quot;\x01&quot;)[int] output_profile_id ;
    //   record
    //     string(&quot;\x01&quot;) output_profile_name ;
    //     string(&quot;\x01&quot;) [int] alias_names;
    //     string(&quot;\x01&quot;) [int] job_names;
    //     string(&quot;\x01&quot;)[int] formulary_names ;
    //     string(&quot;\x01&quot;) [int] form_override_data_paths;
    //     string(&quot;\x01&quot;)[int] formulary_pseudonyms ;
    //     string(&quot;\x01&quot;) tal_name ;
    //     string(&quot;\x01&quot;) tac_name ;
    //     string(&quot;\x01&quot;) tar_name ;
    //     string(&quot;\x01&quot;) tsd_name ;
    //     string(&quot;\x01&quot;, maximum_length=1) st_tac_ind ;
    //   end[int] op_dtls;
    //   string(&quot;\x01&quot;)[int] err_msgs ;
    //   string(1) newline = &quot;\n&quot;;
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
    //   decimal(&quot;\x01&quot;,0, maximum_length=16) alt_run_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=16) job_run_id ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) rec_crt_ts ;
    //   string(&quot;\x01&quot;, maximum_length=30) rec_crt_user_id ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type alias_dtl_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) alias_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) alias_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) alias_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) qual_priority = 99;
    //   string(&quot;\x01&quot;, maximum_length=10) qual_id_type_cd = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=14) qual_id_value = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=70) search_txt ;
    //   string(&quot;\x01&quot;, maximum_length=70) replace_txt = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) rank ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type alias_info_t =  record
    //         decimal(&quot;\x01&quot;,0, maximum_length=39) qual_priority = 99;
    //         string(&quot;\x01&quot;, maximum_length=14) qual_id_value = NULL(&quot;&quot;) ;
    //         string(&quot;\x01&quot;, maximum_length=70) search_txt ;
    //         string(&quot;\x01&quot;, maximum_length=70) replace_txt  ;
    //         date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt;
    //         date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt;
    // end;
    // type alias_xwalk_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) alias_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) alias_name ;
    //   alias_info_t[int] alias_info;
    // end;
    // type target_alt_rank_t =
    // record
    //     int target_dl_bit;
    //     decimal('\x01') bucket_index = 0;
    //     decimal('\x01') udl_index = 0;
    //     string(20) tar_udl_nm =NULL(&quot;&quot;);
    //     decimal('\x01') tac_index = 0;
    //     record
    //       int alt_rank;
    //       int[int] alt_prdcts;
    //       string(2) constituent_group = NULL(&quot;&quot;);
    //       string(1) constituent_reqd = NULL(&quot;&quot;);
    //       string(20) udl_nm =NULL(&quot;&quot;);
    //     end[int] alt_prdcts_rank;
    //     string(&quot;\x01&quot;)has_alt;
    //     decimal(1) ST_flag =0;
    // end;
    // type alias_content_xwalk_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=13) alias_set_name;
    //   string(&quot;\x01&quot;, maximum_length=11)  ndc11 ;
    //   string(&quot;\x01&quot;) alias_label_nm ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type gpi_rank_ratio_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=14) gpi14 ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) rank ;
    //   decimal(&quot;\x01&quot;.3, maximum_length=6) ratio = NULL(&quot;&quot;) ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) run_eff_dt = NULL(&quot;&quot;) ;
    //  end;
    // type tad_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tad_id ;
    //   string(&quot;\x01&quot;, maximum_length=14) target_gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=12) alt_grouping_gpi12 ;
    //   string(&quot;\x01&quot;, maximum_length=14) alt_selection_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) rank ;
    //   decimal(&quot;\x01&quot;.3, maximum_length=7) qty_adjust_factor = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tad_xwalk_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=14) target_gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=6) alt_selection_cd;
    //   string(&quot;\x01&quot;, maximum_length=14)[int] alt_selection_ids ;
    //   record
    //     decimal(&quot;\x01&quot;,0, maximum_length=39) rank ;
    //     decimal(&quot;\x01&quot;.3, maximum_length=7) qty_adjust_factor = NULL(&quot;&quot;) ;
    //   end[int] tad_alt_dtls ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type step_grp_xwalk_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=11) ndc11 ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) step_therapy_step_number = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=100)[int] step_therapy_group_names  ;
    // end;
    // type step_grp_num_xwalk_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tac_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tac_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) priority ;
    //   decimal(&quot;\x01&quot;) _target_st_grp_num = allocate();
    //   decimal(&quot;\x01&quot;)[int] _alt_st_grp_nums = allocate();
    // end;
    // type tal_assoc_clinical_indn_t =
    // record
    //  decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_id ;
    //  string(&quot;\x01&quot;, maximum_length=40) tal_assoc_name ;
    //  decimal(&quot;\x01&quot;,0, maximum_length=10) clinical_indn_id = NULL(&quot;&quot;);
    //  string(&quot;\x01&quot;, maximum_length=20) clinical_indn_name ;
    //  string(&quot;\x01&quot;, maximum_length=200) clinical_indn_desc = NULL(&quot;&quot;);
    //  decimal(&quot;\x01&quot;,0, maximum_length=39) rank = NULL(&quot;&quot;);
    // end;
    // type alt_run_clinical_indn_dtl_load_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=16) alt_run_id = -1 ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=16) alt_run_target_dtl_id = -1 ;
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_name ;
    //   string(&quot;\x01&quot;, maximum_length=11) target_ndc ;
    //   string(&quot;\x01&quot;, maximum_length=40) tal_assoc_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) rank ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) rec_crt_ts ;
    //   string(&quot;\x01&quot;, maximum_length=30) rec_crt_user_id ;
    //   string(&quot;\x01&quot;, maximum_length=200) clinical_indn_desc = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type form_target_reject_t =
    // record
    // string(&quot;,&quot;) error_msg;
    // decimal(&quot;,&quot;) tal_id ;
    // string(&quot;,&quot;) tal_name ;
    // string(&quot;,&quot;) tal_assoc_name = NULL(&quot;&quot;) ;
    // string(&quot;,&quot;) ndc11 = NULL(&quot;&quot;);
    // string(&quot;,&quot;) gpi14 = NULL(&quot;&quot;);
    // string(&quot;\n&quot;) prod_short_desc = NULL(&quot;&quot;);
    // end;
    // type form_alt_reject_t =
    // record
    // string(&quot;,&quot;) error_msg;
    // decimal(&quot;,&quot;) tal_id ;
    // string(&quot;,&quot;) tal_name ;
    // string(&quot;,&quot;) tal_assoc_name = NULL(&quot;&quot;) ;
    // string(&quot;,&quot;) target_ndc11 = NULL(&quot;&quot;);
    // string(&quot;,&quot;) alt_ndc11 = NULL(&quot;&quot;);
    // string(&quot;,&quot;) gpi14 = NULL(&quot;&quot;);
    // string(&quot;\n&quot;) prod_short_desc = NULL(&quot;&quot;);
    // end;
    // type tal_assoc_enriched_data_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tal_assoc_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=20) Target_Alternative;
    //   string(&quot;\x01&quot;, maximum_length=11)  ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=14)  gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=1)  msc ;
    //   string(&quot;\x01&quot;, maximum_length=70)  drug_name ;
    //   string(&quot;\x01&quot;, maximum_length=30) prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=2)  roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4)  dosage_form_cd ;
    //   string(&quot;\x01&quot;, maximum_length=3)  rx_otc ;
    //   string(&quot;\x01&quot;, maximum_length=1)  repack_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1)  status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=8) inactive_dt ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) alt_rank = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=30) shared_qual ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tac_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tar_name = NULL(&quot;&quot;) ;
    //   string(2) constituent_group = NULL(&quot;&quot;);
    //   string(1) constituent_reqd = NULL(&quot;&quot;);
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) constituent_rank = NULL(&quot;&quot;);
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tsd_enriched_data_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tsd_id ;
    //   string(&quot;\x01&quot;, maximum_length=30) tsd_cd ;
    //   string(&quot;\x01&quot;, maximum_length=11)  ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=14)  gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=1)  msc ;
    //   string(&quot;\x01&quot;, maximum_length=70)  drug_name ;
    //   string(&quot;\x01&quot;, maximum_length=30) prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=2)  roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4)  dosage_form_cd ;
    //   string(&quot;\x01&quot;, maximum_length=3)  rx_otc ;
    //   string(&quot;\x01&quot;, maximum_length=1)  repack_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1)  status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=8) inactive_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tac_enriched_data_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tac_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tac_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) priority ;
    //   string(&quot;\x01&quot;, maximum_length=20) Target_Alternative;
    //   string(&quot;\x01&quot;, maximum_length=11)  ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=14)  gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=1)  msc ;
    //   string(&quot;\x01&quot;, maximum_length=70)  drug_name ;
    //   string(&quot;\x01&quot;, maximum_length=30) prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=2)  roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4)  dosage_form_cd ;
    //   string(&quot;\x01&quot;, maximum_length=3)  rx_otc ;
    //   string(&quot;\x01&quot;, maximum_length=1)  repack_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1)  status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=8) inactive_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tar_enriched_data_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tar_id  ;
    //   string(&quot;\x01&quot;, maximum_length=20) tar_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) Target_Alternative;
    //   string(&quot;\x01&quot;, maximum_length=11)  ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=14)  gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=1)  msc ;
    //   string(&quot;\x01&quot;, maximum_length=70)  drug_name ;
    //   string(&quot;\x01&quot;, maximum_length=30) prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=2)  roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4)  dosage_form_cd ;
    //   string(&quot;\x01&quot;, maximum_length=3)  rx_otc ;
    //   string(&quot;\x01&quot;, maximum_length=1)  repack_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1)  status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=8) inactive_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type asso_enriched_tac_data_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) Target_Alternative;
    //   string(&quot;\x01&quot;, maximum_length=11)  ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=14)  gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=1)  msc ;
    //   string(&quot;\x01&quot;, maximum_length=70)  drug_name ;
    //   string(&quot;\x01&quot;, maximum_length=30) prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=2)  roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4)  dosage_form_cd ;
    //   string(&quot;\x01&quot;, maximum_length=3)  rx_otc ;
    //   string(&quot;\x01&quot;, maximum_length=1)  repack_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1)  status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=8) inactive_dt ;
    //   string(&quot;\x01&quot;, maximum_length=30) shared_qual ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tac_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tar_name = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type asso_enriched_sorted_data_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=11)  Target_ndc11 ='N/A';
    //   string(&quot;\x01&quot;, maximum_length=20) Target_Alternative;
    //   string(&quot;\x01&quot;, maximum_length=11)  ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=15) tad_eli_code= NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=14)  gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=1)  msc ;
    //   string(&quot;\x01&quot;, maximum_length=70)  drug_name ;
    //   string(&quot;\x01&quot;, maximum_length=30) prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=2)  roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4)  dosage_form_cd ;
    //   string(&quot;\x01&quot;, maximum_length=3)  rx_otc ;
    //   string(&quot;\x01&quot;, maximum_length=1)  repack_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1)  status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=8) inactive_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type asso_enriched_diff_data_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20) Target_Alternative;
    //   string(&quot;\x01&quot;, maximum_length=11)  ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=14)  gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=1)  msc ;
    //   string(&quot;\x01&quot;, maximum_length=70)  drug_name ;
    //   string(&quot;\x01&quot;, maximum_length=30) prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=2)  roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4)  dosage_form_cd ;
    //   string(&quot;\x01&quot;, maximum_length=3)  rx_otc ;
    //   string(&quot;\x01&quot;, maximum_length=1)  repack_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1)  status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=8) inactive_dt ;
    //   end;
    // type udl_ref_t =
    // record
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) as_of_dt ;
    // end;
    // type output_profile_rebate_dtl_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) output_profile_rebate_dtl_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) udl_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) output_profile_id ;
    //   string(&quot;\x01&quot;, maximum_length=15) rebate_elig_cd ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // metadata type = tac_dtl_t ;</format></in-port>
    //
    //
    //       <out-port><out0><format>type qual_priority_t =
    // record
    //   string(int) qual;
    //   decimal(&quot;&quot;) priority;
    // end;
    // constant string(int)[int] OVERRIDE_QUALIFIERS = [vector 'DESI_CD', 'DOSAGE_FORM', 'MSC', 'ROA', 'RXOTC' ];
    // constant qual_priority_t[int] QUAL_PRIORITY =[vector [record  qual &quot;NDC11&quot;        priority &quot;1&quot;],
    //                                                       [record qual &quot;NDC9&quot;         priority &quot;2&quot;],
    //                                                       [record qual &quot;NDC5&quot;         priority &quot;3&quot;],
    //                                                       [record qual &quot;GPI14&quot;        priority &quot;4&quot;],
    //                                                       [record qual &quot;GPI12&quot;        priority &quot;5&quot;],
    //                                                       [record qual &quot;GPI10&quot;        priority &quot;6&quot;],
    //                                                       [record qual &quot;DOSAGE_FORM&quot;  priority &quot;7&quot;],
    //                                                       [record qual &quot;ROA&quot;          priority &quot;8&quot;],
    //                                                       [record qual &quot;DRUG_NAME&quot;    priority &quot;9&quot;],
    //                                                       [record qual &quot;GPI8&quot;         priority &quot;10&quot;],
    //                                                       [record qual &quot;GPI6&quot;         priority &quot;11&quot;],
    //                                                       [record qual &quot;GPI4&quot;         priority &quot;12&quot;],
    //                                                       [record qual &quot;MSC&quot;          priority &quot;13&quot;],
    //                                                       [record qual &quot;RXOTC&quot;        priority &quot;14&quot;],
    //                                                       [record qual &quot;DAYS_UNTIL_DRUG_STATUS_INACTIVE&quot; priority &quot;15&quot;] ,
    //                                                       [record qual &quot;STATUS_CD&quot;  priority   &quot;16&quot;],
    //                                                       [record qual &quot;REPACKAGER&quot;   priority &quot;17&quot;],
    //                                                       [record qual &quot;DESI_CD&quot;    priority   &quot;18&quot;]];
    // type drug_data_set_dtl_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20) rxclaim_env_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) group = NULL(&quot;&quot;) ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) run_eff_dt = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) drug_data_set_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) drug_data_set_id ;
    //   string(&quot;\x01&quot;, maximum_length=11) ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=14) gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=1) status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=8) eff_dt = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=8) term_dt = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=8) inactive_dt = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=1) msc ;
    //   string(&quot;\x01&quot;, maximum_length=70) drug_name ;
    //   string(&quot;\x01&quot;, maximum_length=30) prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=3) rx_otc ;
    //   string(&quot;\x01&quot;, maximum_length=1) rx_otc_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1) desi ;
    //   string(&quot;\x01&quot;, maximum_length=2) roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4) dosage_form_cd ;
    //   decimal(&quot;\x01&quot;.5, maximum_length=15) prod_strength ;
    //   string(&quot;\x01&quot;, maximum_length=1) repack_cd ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi8_desc ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type user_defined_list_rule_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) user_defined_list_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) user_defined_list_name ;
    //   string(&quot;\x01&quot;, maximum_length=60) user_defined_list_desc ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) user_defined_list_rule_id ;
    //   string(&quot;\x01&quot;, maximum_length=4000) rule = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=1) incl_cd ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type rule_xwalk_t=
    // record
    //   decimal(&quot;\x01&quot;) udl_id;
    //   decimal(&quot;\x01&quot;) udl_rule_id;
    //   string(&quot;\x01&quot;) udl_nm;
    //   string(&quot;\x01&quot;) udl_desc;
    //   decimal(&quot;\x01&quot;) rule_priority;
    //   string(1) inclusion_cd;
    //   string(int) qualifier_cd;
    //   string(int) operator;
    //   string(int) compare_value;
    //   string(1) conjunction_cd;
    //   decimal(&quot;\x01&quot;) rule_expression_id;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type udl_mstr_xwalk_t=
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) udl_id;
    //   string(&quot;\x01&quot;, maximum_length=20) udl_nm;
    //   string(&quot;\x01&quot;, maximum_length=60) user_defined_list_desc;
    //   string(int)[int] qual_list;
    //   decimal(1) override_flg;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type product_lkp_t =
    // record
    //   int dl_bit;
    //   string(&quot;\x01&quot;, maximum_length=11)  ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=14)  gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=1)  status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=8) inactive_dt ;
    //   string(&quot;\x01&quot;, maximum_length=1)  msc ;
    //   string(&quot;\x01&quot;, maximum_length=70)  drug_name ;
    //   string(&quot;\x01&quot;, maximum_length=3)  rx_otc ;
    //   string(&quot;\x01&quot;, maximum_length=1)  desi ;
    //   string(&quot;\x01&quot;, maximum_length=2)  roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4)  dosage_form_cd ;
    //   decimal(&quot;\x01&quot;.5, maximum_length=15) prod_strength ;
    //   string(&quot;\x01&quot;, maximum_length=1)  repack_cd ;
    //   string(&quot;\x01&quot;, maximum_length=30) prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi8_desc ;
    //   string(1) newline = &quot;\n&quot;;
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
    //   string(&quot;\x01&quot;, maximum_length=20)  carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20)  account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20)  group = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=11)  ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=14)  gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=1)  status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=8) eff_dt = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=8) term_dt = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=8) inactive_dt ;
    //   string(&quot;\x01&quot;, maximum_length=1)  msc ;
    //   string(&quot;\x01&quot;, maximum_length=70)  drug_name ;
    //   string(&quot;\x01&quot;, maximum_length=3)  rx_otc ;
    //   string(&quot;\x01&quot;, maximum_length=1)  desi ;
    //   string(&quot;\x01&quot;, maximum_length=2)  roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4)  dosage_form_cd ;
    //   decimal(&quot;\x01&quot;.5, maximum_length=15) prod_strength ;
    //   string(&quot;\x01&quot;, maximum_length=1)  repack_cd ;
    //   string(&quot;\x01&quot;, maximum_length=30) prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi8_desc ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type cag_ovrrd_ref_file_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20)  carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20)  account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20)  group = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=1) is_future_snap = 0;
    //   string(&quot;\x01&quot;) data_path;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type udl_exp_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) udl_id;
    //   string(&quot;\x01&quot;, maximum_length=20) udl_nm;
    //   string(&quot;\x01&quot;, maximum_length=60) udl_desc;
    //   bit_vector_t products;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
    //   bit_vector_t[int] contents;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tal_dtl_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_name ;
    //   string(&quot;\x01&quot;, maximum_length=60) tal_desc ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tal_dtl_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=20) nested_tal_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;, 6, maximum_length=39) priority ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tal_assoc_dtl_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name ;
    //   string(&quot;\x01&quot;, maximum_length=60) tal_assoc_desc ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tal_assoc_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=20) target_udl_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) alt_udl_name = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) alt_rank = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) constituent_rank = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=2) constituent_group = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=1) constituent_reqd = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=30) shared_qual ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tac_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tar_name = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tac_dtl_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tac_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tac_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tac_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) priority ;
    //   string(&quot;\x01&quot;, maximum_length=4000) target_rule = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=4000) alt_rule = NULL(&quot;&quot;) ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tac_rule_xwalk_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tac_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tac_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) priority ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
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
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type contents_alt_st =
    // record
    //  bit_vector_t target_prdcts;
    //  bit_vector_t alternate_prdcts;
    //  string(&quot;\x01&quot;, maximum_length=40) tal_assoc_name;
    // end;
    // type tac_st_contents =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_id;
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
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tac_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tac_name ;
    //   tac_contents_t[int] tac_contents;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
    //   string(1) newline = '\n';
    // end;
    // type tal_container_xwalk_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20) tal_name ;
    //   int target_dl_bit;
    //   record
    //    int alt_prd;
    //    int alt_rank;
    //   end[int] alt_prdcts;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type target_udl_products_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20) target_udl_name = NULL(&quot;&quot;) ;
    //   bit_vector_t target_products;
    // end;
    // type alt_udl_products_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20) alt_udl_name = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) alt_rank = NULL(&quot;&quot;) ;
    //   bit_vector_t alt_products;
    // end;
    // type alt_exclusion_products_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tal_assoc_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tac_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tar_name = NULL(&quot;&quot;) ;
    //   bit_vector_t target_product_dtl;
    //   bit_vector_t alt_product_dtl;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tal_assoc_xwalk_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name ;
    //   string(&quot;\x01&quot;, maximum_length=30) shared_qual ;
    //   string(&quot;\x01&quot;, maximum_length=10) override_tac_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=10) override_tar_name = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tal_assoc_type_cd ;
    //   target_udl_products_t[int] target_udl_products;
    //   alt_udl_products_t[int] alt_udl_products;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type form_data_set_dtl_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) formulary_data_set_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_id = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=10) formulary_cd = &quot;&quot; ;
    //   string(&quot;\x01&quot;, maximum_length=20) carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) group = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) rxclaim_env_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) customer_name = NULL(&quot;&quot;);
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) last_exp_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) run_eff_dt = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) formulary_data_set_dtl_id ;
    //   string(&quot;\x01&quot;, maximum_length=11) ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=2) formulary_tier ;
    //   string(&quot;\x01&quot;, maximum_length=2) formulary_status ;
    //   string(&quot;\x01&quot;, maximum_length=1) pa_reqd_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) specialty_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) step_therapy_ind ;
    //   string(&quot;\x01&quot;, maximum_length=50) formulary_tier_desc ;
    //   string(&quot;\x01&quot;, maximum_length=50) formulary_status_desc ;
    //   string(&quot;\x01&quot;, maximum_length=1) pa_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1) step_therapy_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=100) step_therapy_group_name = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) step_therapy_step_number = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type form_ovrrd_xwalk_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_name ;
    //   string(&quot;\x01&quot;, maximum_length=10) formulary_cd = &quot;&quot; ;
    //   string(&quot;\x01&quot;, maximum_length=20)  carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20)  account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20)  group = NULL(&quot;&quot;) ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) last_exp_dt ;
    //   string(&quot;\x01&quot;, maximum_length=11) ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=2) formulary_tier ;
    //   string(&quot;\x01&quot;, maximum_length=2) formulary_status ;
    //   string(&quot;\x01&quot;, maximum_length=1) pa_reqd_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) specialty_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) step_therapy_ind ;
    //   string(&quot;\x01&quot;, maximum_length=50) formulary_tier_desc ;
    //   string(&quot;\x01&quot;, maximum_length=50) formulary_status_desc ;
    //   string(&quot;\x01&quot;, maximum_length=1) pa_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1) step_therapy_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=100) step_therapy_group_name = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) step_therapy_step_number = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type form_ovrrd_ref_file_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_name ;
    //   string(&quot;\x01&quot;, maximum_length=20)  carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20)  account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20)  group = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) customer_name = NULL(&quot;&quot;);
    //   decimal(&quot;\x01&quot;,0, maximum_length=1) is_future_snap = 0;
    //   string(&quot;\x01&quot;) data_path;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) run_eff_dt = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type output_profile_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) output_profile_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) rxclaim_env_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_id = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) output_profile_form_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) output_profile_job_dtl_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) output_profile_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) alias_name = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) alias_priority = NULL ;
    //   string(&quot;\x01&quot;, maximum_length=20) carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) group = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) tac_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) tar_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) tsd_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) job_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) job_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) customer_name = NULL(&quot;&quot;);
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) run_day = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) lob_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=4) run_jan1_start_mmdd = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=4) run_jan1_end_mmdd = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;) future_flg = 0;
    //   string(&quot;\x01&quot;, maximum_length=30) formulary_pseudonym = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) notes_id = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=60) output_profile_desc ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39 ) formulary_option_cd ;
    //   string(&quot;\x01&quot;, maximum_length=20) layout_name ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) as_of_dt = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=1) st_tac_ind ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type form_product_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=10) formulary_cd = &quot;&quot; ;
    //   string(&quot;\x01&quot;, maximum_length=11) ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=2) formulary_tier ;
    //   string(&quot;\x01&quot;, maximum_length=2) formulary_status ;
    //   string(&quot;\x01&quot;, maximum_length=1) pa_reqd_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) specialty_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) step_therapy_ind ;
    //   string(&quot;\x01&quot;, maximum_length=50) formulary_tier_desc ;
    //   string(&quot;\x01&quot;, maximum_length=50) formulary_status_desc ;
    //   string(&quot;\x01&quot;, maximum_length=1) pa_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1) step_therapy_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=100) step_therapy_group_name = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) step_therapy_step_number = NULL(&quot;&quot;) ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) last_exp_dt ;
    // end;
    // type cag_product_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=11) ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=14) gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=1) status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=8) eff_dt = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=8) term_dt = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=8) inactive_dt ;
    //   string(&quot;\x01&quot;, maximum_length=1) msc ;
    //   string(&quot;\x01&quot;, maximum_length=70) drug_name ;
    //   string(&quot;\x01&quot;, maximum_length=3) rx_otc ;
    //   string(&quot;\x01&quot;, maximum_length=1) desi ;
    //   string(&quot;\x01&quot;, maximum_length=2) roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4) dosage_form_cd ;
    //   decimal(&quot;\x01&quot;.5, maximum_length=15) prod_strength ;
    //   string(&quot;\x01&quot;, maximum_length=1) repack_cd ;
    //   string(&quot;\x01&quot;, maximum_length=30) prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi8_desc ;
    // end;
    // type cag_rlp_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20) carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) group = NULL(&quot;&quot;) ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) run_eff_dt = NULL(&quot;&quot;) ;
    //   decimal(1) cag_priority = 0;
    //   cag_product_t[int] prdcts;
    // end;
    // type form_rlp_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) group = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) customer_name = NULL(&quot;&quot;);
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) run_eff_dt = NULL(&quot;&quot;) ;
    //   decimal(1) cag_priority = 0;
    //   form_product_t[int] prdcts;
    // end;
    // type tsd_dtl_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tsd_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tsd_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tsd_name ;
    //   string(&quot;\x01&quot;, maximum_length=30) tsd_cd ;
    //   string(&quot;\x01&quot;, maximum_length=2) formulary_tier ;
    //   string(&quot;\x01&quot;, maximum_length=2) formulary_status ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) priority ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tsd_xwalk_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tsd_id ;
    //   string(&quot;\x01&quot;, maximum_length=30) tsd_cd ;
    //   bit_vector_t products;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type alt_constituent_prdct_t =
    // record
    //   bit_vector_t alt_prdcts;
    //   string(2) constituent_group = NULL(&quot;&quot;);
    //   string(1) constituent_reqd = NULL(&quot;&quot;);
    //   string(20) udl_nm = NULL(&quot;&quot;);
    // end;
    // type tal_container_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) tar_udl_nm = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=60) tal_desc ;
    //   decimal(&quot;\x01&quot;, 6, maximum_length=39) priority ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tal_assoc_type_cd ;
    //   bit_vector_t[int] target_prdcts;
    //   alt_constituent_prdct_t[int] alt_constituent_prdcts;
    //   string(&quot;\x01&quot;, maximum_length=30) shared_qual ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tac_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tar_name = NULL(&quot;&quot;) ;
    //   string(2)[int] constituent_grp_vec;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type master_cag_mapping_t =
    // record
    //   string(&quot;\x01&quot;) carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;) account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;) group = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;) future_flg = 'C';
    //   string(&quot;\x01&quot;) cag_override_data_path= NULL(&quot;&quot;);
    //   decimal(&quot;\x01&quot;)[int] qual_output_profile_ids ;
    //   record
    //     string(&quot;\x01&quot;) qual_output_profile_name ;
    //     string(&quot;\x01&quot;) rxclaim_env_name;
    //     decimal(&quot;\x01&quot;) [int] job_ids;
    //     string(&quot;\x01&quot;) [int] alias_names;
    //     string(&quot;\x01&quot;) [int] job_names;
    //     string(&quot;\x01&quot;) [int] formulary_names ;
    //     string(&quot;\x01&quot;) [int] form_override_data_paths;
    //     string(&quot;\x01&quot;, maximum_length=30)[int] formulary_pseudonyms;
    //     string(&quot;\x01&quot;, maximum_length=20) tal_name ;
    //     string(&quot;\x01&quot;) tac_name ;
    //     string(&quot;\x01&quot;) tar_name ;
    //     string(&quot;\x01&quot;) tsd_name ;
    //     string(&quot;\x01&quot;, maximum_length=1) st_tac_ind ;
    //   end[int] op_dtls;
    //   record
    //     decimal(&quot;\x01&quot;) non_qual_op_id;
    //     string(&quot;\x01&quot;) rxclaim_env_name;
    //     decimal(&quot;\x01&quot;) [int] job_ids;
    //     string(&quot;\x01&quot;)[int] formulary_names;
    //     string(&quot;\x01&quot;, maximum_length=30)[int] formulary_pseudonyms;
    //     string(&quot;\x01&quot;) [int] alias_names;
    //     string(&quot;\x01&quot;) [int] job_names;
    //   end[int] non_qual_output_profile_ids ;
    //   string(&quot;\x01&quot;) [int] err_msgs;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) as_of_dt = NULL(&quot;&quot;);
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tar_dtl_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tar_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tar_dtl_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tar_name ;
    //   string(&quot;\x01&quot;, maximum_length=1) sort_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) filter_ind ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) priority ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tar_dtl_type_cd ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tar_roa_df_set_id = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=4000) target_rule = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=4000) alt_rule = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=15) rebate_elig_cd = NULL(&quot;&quot;) ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tar_roa_df_set_dtl_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tar_roa_df_set_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tar_roa_df_set_id ;
    //   string(&quot;\x01&quot;, maximum_length=2) target_roa_cd = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=4) target_dosage_form_cd = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=2) alt_roa_cd = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=4) alt_dosage_form_cd = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) priority ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tar_rule_xwalk_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tar_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tar_dtl_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tar_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tar_roa_df_set_id = NULL(&quot;&quot;);
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tar_dtl_type_cd ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) priority ;
    //   string(&quot;\x01&quot;, maximum_length=1) sort_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) filter_ind ;
    //   string(&quot;\x01&quot;, maximum_length=15) rebate_elig_cd = NULL(&quot;&quot;) ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
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
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type alt_run_alt_dtl_load_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=16 ) alt_run_alt_dtl_id = -1 ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=16 ) alt_run_id = -1;
    //   decimal(&quot;\x01&quot;,0, maximum_length=16 ) alt_run_target_dtl_id = -1 ;
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_name ;
    //   string(&quot;\x01&quot;, maximum_length=11) target_ndc ;
    //   string(&quot;\x01&quot;, maximum_length=11) alt_ndc ;
    //   string(&quot;\x01&quot;, maximum_length=2) alt_formulary_tier ;
    //   string(&quot;\x01&quot;, maximum_length=1) alt_multi_src_cd ;
    //   string(&quot;\x01&quot;, maximum_length=2) alt_roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4) alt_dosage_form_cd ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39 ) rank ;
    //   string(&quot;\x01&quot;, maximum_length=1) alt_step_therapy_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) alt_pa_reqd_ind ;
    //   string(&quot;\x01&quot;, maximum_length=2) alt_formulary_status ;
    //   string(&quot;\x01&quot;, maximum_length=1) alt_specialty_ind ;
    //   string(&quot;\x01&quot;, maximum_length=14) alt_gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=70) alt_prod_name_ext ;
    //   string(&quot;\x01&quot;, maximum_length=30) alt_prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=30) alt_prod_short_desc_grp;
    //   string(&quot;\x01&quot;, maximum_length=60) alt_gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) alt_gpi8_desc ;
    //   string(&quot;\x01&quot;, maximum_length=50) alt_formulary_tier_desc ;
    //   string(&quot;\x01&quot;, maximum_length=50) alt_formulary_status_desc ;
    //   string(&quot;\x01&quot;, maximum_length=1) alt_pa_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1) alt_step_therapy_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1000) alt_step_therapy_group_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=100) alt_step_therapy_step_number = NULL(&quot;&quot;) ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) rec_crt_ts ;
    //   string(&quot;\x01&quot;, maximum_length=30) rec_crt_user_id ;
    //   string(&quot;\x01&quot;, maximum_length=15) rebate_elig_cd = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=15) tad_eligible_cd= NULL(&quot;&quot;);
    //   decimal(&quot;\x01&quot;.3, maximum_length=7) alt_qty_adj = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=40) tal_assoc_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=40) tala = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) alt_udl = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;.6, maximum_length=39) tal_assoc_rank ;
    //   string(&quot;\x01&quot;, maximum_length=2) constituent_group = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=1) constituent_reqd = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type alt_run_target_dtl_load_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=16 ) alt_run_target_dtl_id = -1 ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=16 ) alt_run_id  = -1 ;
    //   string(&quot;\x01&quot;, maximum_length=40) tal_assoc_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=40) tala = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) tar_udl = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_name ;
    //   string(&quot;\x01&quot;, maximum_length=11) target_ndc ;
    //   string(&quot;\x01&quot;, maximum_length=2) target_formulary_tier ;
    //   string(&quot;\x01&quot;, maximum_length=2) target_formulary_status ;
    //   string(&quot;\x01&quot;, maximum_length=1) target_pa_reqd_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) target_step_therapy_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) target_specialty_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) target_multi_src_cd ;
    //   string(&quot;\x01&quot;, maximum_length=2) target_roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4) target_dosage_form_cd ;
    //   string(&quot;\x01&quot;, maximum_length=14) target_gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=70) target_prod_name_ext ;
    //   string(&quot;\x01&quot;, maximum_length=30) target_prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) target_gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) target_gpi8_desc ;
    //   string(&quot;\x01&quot;, maximum_length=50) target_formulary_tier_desc ;
    //   string(&quot;\x01&quot;, maximum_length=50) target_formulary_status_desc ;
    //   string(&quot;\x01&quot;, maximum_length=1) target_pa_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1) target_step_therapy_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1000) target_step_therapy_group_name = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) target_step_therapy_step_num = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=10) formulary_cd = NULL(&quot;&quot;) ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) rec_crt_ts ;
    //   string(&quot;\x01&quot;, maximum_length=30) rec_crt_user_id ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) last_exp_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type alt_run_load_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=16) alt_run_id = -1 ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) output_profile_id ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) run_start_ts = NULL(&quot;&quot;) ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) run_complete_ts = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) alt_run_status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1) published_ind ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) rec_crt_ts ;
    //   string(&quot;\x01&quot;, maximum_length=30) rec_crt_user_id ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) rec_last_upd_ts ;
    //   string(&quot;\x01&quot;, maximum_length=30) rec_last_upd_user_id ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) run_eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) as_of_dt = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type alt_run_load_adhoc_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=16) alt_run_id = -1 ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) output_profile_id ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) run_start_ts = NULL(&quot;&quot;) ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) run_complete_ts = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) alt_run_status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1) published_ind ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) rec_crt_ts ;
    //   string(&quot;\x01&quot;, maximum_length=30) rec_crt_user_id ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) rec_last_upd_ts ;
    //   string(&quot;\x01&quot;, maximum_length=30) rec_last_upd_user_id ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) run_eff_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type alt_run_t =
    // record
    //   decimal(&quot;|&quot;) alt_run_id ;
    //   decimal(&quot;|&quot;) output_profile_id ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;|&quot;) run_start_ts = NULL(&quot;&quot;) ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;|&quot;) run_complete_ts = NULL(&quot;&quot;) ;
    //   decimal(&quot;|&quot;) alt_run_status_cd ;
    //   string(&quot;|&quot;) published_ind ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;|&quot;) rec_crt_ts ;
    //   string(&quot;|&quot;) rec_crt_user_id ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;|&quot;) rec_last_upd_ts ;
    //   string(&quot;|&quot;) rec_last_upd_user_id ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\n&quot;) run_eff_dt ;
    // end;
    // type alt_run_alt_proxy_dtl_load_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=16 ) alt_run_id = -1;
    //   decimal(&quot;\x01&quot;,0, maximum_length=16 ) alt_run_alt_dtl_id = -1 ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=16 ) alt_run_target_dtl_id = -1 ;
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_name ;
    //   string(&quot;\x01&quot;, maximum_length=11) target_ndc ;
    //   string(&quot;\x01&quot;, maximum_length=40) tal_assoc_name ;
    //   string(&quot;\x01&quot;, maximum_length=11) alt_proxy_ndc ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39 ) rank ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) rec_crt_ts ;
    //   string(&quot;\x01&quot;, maximum_length=30) rec_crt_user_id ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type file_load_cntl_load_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=50) user_id ;
    //   string(&quot;\x01&quot;, maximum_length=1) file_load_type_cd ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) component_type_cd = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) file_load_status_cd ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) file_load_start_ts = NULL(&quot;&quot;) ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) rec_crt_ts ;
    //   string(&quot;\x01&quot;, maximum_length=30) rec_crt_user_id ;
    //   string(&quot;\x01&quot;, maximum_length=1) published_ind ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=16) alt_run_id = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type file_load_ctl_dtl_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=16) file_load_cntl_id ;
    //   string(&quot;\x01&quot;, maximum_length=4000) component_ids = NULL(&quot;&quot;) ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) as_of_date ;
    //   string(&quot;\x01&quot;, maximum_length=20) rxclaim_env_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) group = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) component_type_cd = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=512) file_name_w = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=1) published_ind ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=16) alt_run_id = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=512) report_file_name = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type file_load_err_dtl_load_t =
    // record
    //   decimal(&quot;,&quot;,0, maximum_length=16) file_load_cntl_id ;
    //   string(&quot;,&quot;, maximum_length=1024) err_desc ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;,&quot;) rec_crt_ts ;
    //   string(&quot;,&quot;, maximum_length=30) rec_crt_user_id ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;,&quot;) rec_last_upd_ts ;
    //   string(&quot;\n&quot;, maximum_length=30) rec_last_upd_user_id ;
    // end;
    // type tal_assoc_prdcts_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tal_assoc_type_cd ;
    //   bit_vector_t[int] target_prdcts;
    //   bit_vector_t[int] alt_prdcts;
    //   string(&quot;\x01&quot;, maximum_length=30) shared_qual ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tac_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tar_name = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type part_exp_master_file_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=16) file_load_cntl_id ;
    //   string(&quot;\x01&quot;, maximum_length=1024) component_ids ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) as_of_date ;
    //   string(&quot;\x01&quot;, maximum_length=20) rxclaim_env_name = &quot;&quot; ;
    //   string(&quot;\x01&quot;, maximum_length=50) cag_in = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=50) cag_out = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39, sign_reserved) component_type_cd = NULL(&quot;&quot;) ;
    //   utf8 string(&quot;\x01&quot;, maximum_length=512) file_name_w = NULL(&quot;&quot;) ;
    //   utf8 string(&quot;\x01&quot;, maximum_length=1) published_ind ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type alt_rank_t =
    // record
    //   int alt_rank;
    //   int[int] alt_prdcts;
    //   string(2) constituent_group = NULL(&quot;&quot;);
    //   string(1) constituent_reqd = NULL(&quot;&quot;);
    //   string(20) udl_nm = NULL(&quot;&quot;);
    // end;
    // type rebate_elig_contents_t =
    // record
    //   string(&quot;\x01&quot;) rebate_elig_cd;
    //   int[int] rebate_elig_prdcts;
    // end;
    // type target_prdct_t =
    // record
    //   int target_dl_bit;
    //   decimal('\x01') bucket_index = 0;
    //   decimal('\x01') udl_index = 0;
    //   decimal('\x01') tac_index = 0;
    //   alt_rank_t[int] alt_prdcts_rank;
    //   string(&quot;\x01&quot;)has_alt;
    //   decimal(1) ST_flag = 0;
    // end;
    // type tal_assoc_adhoc_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name ;
    //   string(&quot;\x01&quot;, maximum_length=2000) clinical_indn_desc = NULL(&quot;&quot;);
    //   string(&quot;\x01&quot;, maximum_length=60) tal_assoc_desc ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tal_assoc_type_cd ;
    //   record
    //     string(&quot;\x01&quot;, maximum_length=20) udl_nm;
    //     string(&quot;\x01&quot;, maximum_length=60) udl_desc;
    //     bit_vector_t products;
    //   end[int] target_udl_info ;
    //   record
    //      string(&quot;\x01&quot;, maximum_length=20) udl_nm;
    //      string(&quot;\x01&quot;, maximum_length=60) udl_desc;
    //      bit_vector_t products;
    //      string(2) constituent_group = NULL(&quot;&quot;);
    //      string(1) constituent_reqd = NULL(&quot;&quot;);
    //      decimal(&quot;\x01&quot;,0, maximum_length=39) constituent_rank = NULL(&quot;&quot;);
    //   end[int] alt_udl_info;
    //   string(&quot;\x01&quot;, maximum_length=30) shared_qual ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tac_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tar_name = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tal_container_adhoc_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=2000) clinical_indn_desc = NULL(&quot;&quot;);
    //   string(&quot;\x01&quot;, maximum_length=60) tal_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) tal_assoc_desc ;
    //   decimal(&quot;\x01&quot;, 6, maximum_length=39) priority ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tal_assoc_type_cd ;
    //   record
    //       string(&quot;\x01&quot;, maximum_length=20) udl_nm;
    //       string(&quot;\x01&quot;, maximum_length=60) udl_desc;
    //       bit_vector_t products;
    //   end[int] target_prdcts;
    //   record
    //       string(&quot;\x01&quot;, maximum_length=20) udl_nm;
    //       string(&quot;\x01&quot;, maximum_length=60) udl_desc;
    //       bit_vector_t products;
    //       string(2) constituent_group = NULL(&quot;&quot;);
    //       string(1) constituent_reqd = NULL(&quot;&quot;);
    //       decimal(&quot;\x01&quot;,0, maximum_length=39) constituent_rank = NULL(&quot;&quot;);
    //   end[int] alt_prdcts;
    //   string(&quot;\x01&quot;, maximum_length=30) shared_qual ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tac_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tar_name = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tal_assoc_adhoc_enrich_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name ;
    //   string(&quot;\x01&quot;, maximum_length=2000) clinical_indn_desc = NULL(&quot;&quot;);
    //   string(&quot;\x01&quot;, maximum_length=60) tal_assoc_desc ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tal_assoc_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=20) udl_id = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=60) udl_desc;
    //   string(&quot;\x01&quot;, maximum_length=20) Target_Alternative;
    //   bit_vector_t products;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) alt_rank = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=30) shared_qual ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tac_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tar_name = NULL(&quot;&quot;) ;
    //   string(2) constituent_group = NULL(&quot;&quot;);
    //   string(1) constituent_reqd = NULL(&quot;&quot;);
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) constituent_rank = NULL(&quot;&quot;);
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tal_container_adhoc_enrich_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20) tal_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_id = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=2000) clinical_indn_desc = NULL(&quot;&quot;);
    //   string(&quot;\x01&quot;, maximum_length=60) tal_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) tal_assoc_desc ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tal_assoc_type_cd ;
    //   decimal(&quot;\x01&quot;, 6, maximum_length=39) tal_assoc_rank = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) udl_id = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=60) udl_desc;
    //   string(&quot;\x01&quot;, maximum_length=20) Target_Alternative;
    //   bit_vector_t products;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) alt_rank = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=30) shared_qual ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tac_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tar_name = NULL(&quot;&quot;) ;
    //   string(2) constituent_group = NULL(&quot;&quot;);
    //   string(1) constituent_reqd = NULL(&quot;&quot;);
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) constituent_rank = NULL(&quot;&quot;);
    //   string(1) newline = &quot;\n&quot;;
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
    //   decimal(&quot;\x01&quot;,0, maximum_length=16 ) alt_run_target_dtl_id ;
    //   string(&quot;\x01&quot;, maximum_length=40) tal_assoc_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_name ;
    //   string(&quot;\x01&quot;, maximum_length=11) target_ndc ;
    // end;
    // type alt_run_alt_dtl_ref_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=16 ) alt_run_alt_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=16 ) alt_run_target_dtl_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_name ;
    //   string(&quot;\x01&quot;, maximum_length=40) tal_assoc_name ;
    //   string(&quot;\x01&quot;, maximum_length=11) target_ndc ;
    //   string(&quot;\x01&quot;, maximum_length=11) alt_ndc ;
    // end;
    // type master_cag_mapping_adhoc_t =
    // record
    //   string(&quot;\x01&quot;) carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;) account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;) group = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;) cag_override_data_path= NULL(&quot;&quot;);
    //   decimal(&quot;\x01&quot;)[int] output_profile_id ;
    //   record
    //     string(&quot;\x01&quot;) output_profile_name ;
    //     string(&quot;\x01&quot;) [int] alias_names;
    //     string(&quot;\x01&quot;) [int] job_names;
    //     string(&quot;\x01&quot;)[int] formulary_names ;
    //     string(&quot;\x01&quot;) [int] form_override_data_paths;
    //     string(&quot;\x01&quot;)[int] formulary_pseudonyms ;
    //     string(&quot;\x01&quot;) tal_name ;
    //     string(&quot;\x01&quot;) tac_name ;
    //     string(&quot;\x01&quot;) tar_name ;
    //     string(&quot;\x01&quot;) tsd_name ;
    //     string(&quot;\x01&quot;, maximum_length=1) st_tac_ind ;
    //   end[int] op_dtls;
    //   string(&quot;\x01&quot;)[int] err_msgs ;
    //   string(1) newline = &quot;\n&quot;;
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
    //   decimal(&quot;\x01&quot;,0, maximum_length=16) alt_run_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=16) job_run_id ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) rec_crt_ts ;
    //   string(&quot;\x01&quot;, maximum_length=30) rec_crt_user_id ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type alias_dtl_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) alias_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) alias_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) alias_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) qual_priority = 99;
    //   string(&quot;\x01&quot;, maximum_length=10) qual_id_type_cd = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=14) qual_id_value = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=70) search_txt ;
    //   string(&quot;\x01&quot;, maximum_length=70) replace_txt = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) rank ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type alias_info_t =  record
    //         decimal(&quot;\x01&quot;,0, maximum_length=39) qual_priority = 99;
    //         string(&quot;\x01&quot;, maximum_length=14) qual_id_value = NULL(&quot;&quot;) ;
    //         string(&quot;\x01&quot;, maximum_length=70) search_txt ;
    //         string(&quot;\x01&quot;, maximum_length=70) replace_txt  ;
    //         date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt;
    //         date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt;
    // end;
    // type alias_xwalk_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) alias_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) alias_name ;
    //   alias_info_t[int] alias_info;
    // end;
    // type target_alt_rank_t =
    // record
    //     int target_dl_bit;
    //     decimal('\x01') bucket_index = 0;
    //     decimal('\x01') udl_index = 0;
    //     string(20) tar_udl_nm =NULL(&quot;&quot;);
    //     decimal('\x01') tac_index = 0;
    //     record
    //       int alt_rank;
    //       int[int] alt_prdcts;
    //       string(2) constituent_group = NULL(&quot;&quot;);
    //       string(1) constituent_reqd = NULL(&quot;&quot;);
    //       string(20) udl_nm =NULL(&quot;&quot;);
    //     end[int] alt_prdcts_rank;
    //     string(&quot;\x01&quot;)has_alt;
    //     decimal(1) ST_flag =0;
    // end;
    // type alias_content_xwalk_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=13) alias_set_name;
    //   string(&quot;\x01&quot;, maximum_length=11)  ndc11 ;
    //   string(&quot;\x01&quot;) alias_label_nm ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type gpi_rank_ratio_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=14) gpi14 ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) rank ;
    //   decimal(&quot;\x01&quot;.3, maximum_length=6) ratio = NULL(&quot;&quot;) ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) run_eff_dt = NULL(&quot;&quot;) ;
    //  end;
    // type tad_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tad_id ;
    //   string(&quot;\x01&quot;, maximum_length=14) target_gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=12) alt_grouping_gpi12 ;
    //   string(&quot;\x01&quot;, maximum_length=14) alt_selection_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) rank ;
    //   decimal(&quot;\x01&quot;.3, maximum_length=7) qty_adjust_factor = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tad_xwalk_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=14) target_gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=6) alt_selection_cd;
    //   string(&quot;\x01&quot;, maximum_length=14)[int] alt_selection_ids ;
    //   record
    //     decimal(&quot;\x01&quot;,0, maximum_length=39) rank ;
    //     decimal(&quot;\x01&quot;.3, maximum_length=7) qty_adjust_factor = NULL(&quot;&quot;) ;
    //   end[int] tad_alt_dtls ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type step_grp_xwalk_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=11) ndc11 ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) step_therapy_step_number = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=100)[int] step_therapy_group_names  ;
    // end;
    // type step_grp_num_xwalk_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tac_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tac_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) priority ;
    //   decimal(&quot;\x01&quot;) _target_st_grp_num = allocate();
    //   decimal(&quot;\x01&quot;)[int] _alt_st_grp_nums = allocate();
    // end;
    // type tal_assoc_clinical_indn_t =
    // record
    //  decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_id ;
    //  string(&quot;\x01&quot;, maximum_length=40) tal_assoc_name ;
    //  decimal(&quot;\x01&quot;,0, maximum_length=10) clinical_indn_id = NULL(&quot;&quot;);
    //  string(&quot;\x01&quot;, maximum_length=20) clinical_indn_name ;
    //  string(&quot;\x01&quot;, maximum_length=200) clinical_indn_desc = NULL(&quot;&quot;);
    //  decimal(&quot;\x01&quot;,0, maximum_length=39) rank = NULL(&quot;&quot;);
    // end;
    // type alt_run_clinical_indn_dtl_load_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=16) alt_run_id = -1 ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=16) alt_run_target_dtl_id = -1 ;
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_name ;
    //   string(&quot;\x01&quot;, maximum_length=11) target_ndc ;
    //   string(&quot;\x01&quot;, maximum_length=40) tal_assoc_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) rank ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) rec_crt_ts ;
    //   string(&quot;\x01&quot;, maximum_length=30) rec_crt_user_id ;
    //   string(&quot;\x01&quot;, maximum_length=200) clinical_indn_desc = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type form_target_reject_t =
    // record
    // string(&quot;,&quot;) error_msg;
    // decimal(&quot;,&quot;) tal_id ;
    // string(&quot;,&quot;) tal_name ;
    // string(&quot;,&quot;) tal_assoc_name = NULL(&quot;&quot;) ;
    // string(&quot;,&quot;) ndc11 = NULL(&quot;&quot;);
    // string(&quot;,&quot;) gpi14 = NULL(&quot;&quot;);
    // string(&quot;\n&quot;) prod_short_desc = NULL(&quot;&quot;);
    // end;
    // type form_alt_reject_t =
    // record
    // string(&quot;,&quot;) error_msg;
    // decimal(&quot;,&quot;) tal_id ;
    // string(&quot;,&quot;) tal_name ;
    // string(&quot;,&quot;) tal_assoc_name = NULL(&quot;&quot;) ;
    // string(&quot;,&quot;) target_ndc11 = NULL(&quot;&quot;);
    // string(&quot;,&quot;) alt_ndc11 = NULL(&quot;&quot;);
    // string(&quot;,&quot;) gpi14 = NULL(&quot;&quot;);
    // string(&quot;\n&quot;) prod_short_desc = NULL(&quot;&quot;);
    // end;
    // type tal_assoc_enriched_data_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tal_assoc_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=20) Target_Alternative;
    //   string(&quot;\x01&quot;, maximum_length=11)  ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=14)  gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=1)  msc ;
    //   string(&quot;\x01&quot;, maximum_length=70)  drug_name ;
    //   string(&quot;\x01&quot;, maximum_length=30) prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=2)  roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4)  dosage_form_cd ;
    //   string(&quot;\x01&quot;, maximum_length=3)  rx_otc ;
    //   string(&quot;\x01&quot;, maximum_length=1)  repack_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1)  status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=8) inactive_dt ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) alt_rank = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=30) shared_qual ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tac_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tar_name = NULL(&quot;&quot;) ;
    //   string(2) constituent_group = NULL(&quot;&quot;);
    //   string(1) constituent_reqd = NULL(&quot;&quot;);
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) constituent_rank = NULL(&quot;&quot;);
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tsd_enriched_data_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tsd_id ;
    //   string(&quot;\x01&quot;, maximum_length=30) tsd_cd ;
    //   string(&quot;\x01&quot;, maximum_length=11)  ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=14)  gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=1)  msc ;
    //   string(&quot;\x01&quot;, maximum_length=70)  drug_name ;
    //   string(&quot;\x01&quot;, maximum_length=30) prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=2)  roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4)  dosage_form_cd ;
    //   string(&quot;\x01&quot;, maximum_length=3)  rx_otc ;
    //   string(&quot;\x01&quot;, maximum_length=1)  repack_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1)  status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=8) inactive_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tac_enriched_data_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tac_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tac_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) priority ;
    //   string(&quot;\x01&quot;, maximum_length=20) Target_Alternative;
    //   string(&quot;\x01&quot;, maximum_length=11)  ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=14)  gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=1)  msc ;
    //   string(&quot;\x01&quot;, maximum_length=70)  drug_name ;
    //   string(&quot;\x01&quot;, maximum_length=30) prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=2)  roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4)  dosage_form_cd ;
    //   string(&quot;\x01&quot;, maximum_length=3)  rx_otc ;
    //   string(&quot;\x01&quot;, maximum_length=1)  repack_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1)  status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=8) inactive_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tar_enriched_data_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tar_id  ;
    //   string(&quot;\x01&quot;, maximum_length=20) tar_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) Target_Alternative;
    //   string(&quot;\x01&quot;, maximum_length=11)  ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=14)  gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=1)  msc ;
    //   string(&quot;\x01&quot;, maximum_length=70)  drug_name ;
    //   string(&quot;\x01&quot;, maximum_length=30) prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=2)  roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4)  dosage_form_cd ;
    //   string(&quot;\x01&quot;, maximum_length=3)  rx_otc ;
    //   string(&quot;\x01&quot;, maximum_length=1)  repack_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1)  status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=8) inactive_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type asso_enriched_tac_data_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) Target_Alternative;
    //   string(&quot;\x01&quot;, maximum_length=11)  ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=14)  gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=1)  msc ;
    //   string(&quot;\x01&quot;, maximum_length=70)  drug_name ;
    //   string(&quot;\x01&quot;, maximum_length=30) prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=2)  roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4)  dosage_form_cd ;
    //   string(&quot;\x01&quot;, maximum_length=3)  rx_otc ;
    //   string(&quot;\x01&quot;, maximum_length=1)  repack_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1)  status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=8) inactive_dt ;
    //   string(&quot;\x01&quot;, maximum_length=30) shared_qual ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tac_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tar_name = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type asso_enriched_sorted_data_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=11)  Target_ndc11 ='N/A';
    //   string(&quot;\x01&quot;, maximum_length=20) Target_Alternative;
    //   string(&quot;\x01&quot;, maximum_length=11)  ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=15) tad_eli_code= NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=14)  gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=1)  msc ;
    //   string(&quot;\x01&quot;, maximum_length=70)  drug_name ;
    //   string(&quot;\x01&quot;, maximum_length=30) prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=2)  roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4)  dosage_form_cd ;
    //   string(&quot;\x01&quot;, maximum_length=3)  rx_otc ;
    //   string(&quot;\x01&quot;, maximum_length=1)  repack_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1)  status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=8) inactive_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type asso_enriched_diff_data_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20) Target_Alternative;
    //   string(&quot;\x01&quot;, maximum_length=11)  ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=14)  gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=1)  msc ;
    //   string(&quot;\x01&quot;, maximum_length=70)  drug_name ;
    //   string(&quot;\x01&quot;, maximum_length=30) prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=2)  roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4)  dosage_form_cd ;
    //   string(&quot;\x01&quot;, maximum_length=3)  rx_otc ;
    //   string(&quot;\x01&quot;, maximum_length=1)  repack_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1)  status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=8) inactive_dt ;
    //   end;
    // type udl_ref_t =
    // record
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) as_of_dt ;
    // end;
    // type output_profile_rebate_dtl_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) output_profile_rebate_dtl_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) udl_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) output_profile_id ;
    //   string(&quot;\x01&quot;, maximum_length=15) rebate_elig_cd ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // metadata type =
    // record
    // decimal(&quot;\x01&quot;, 0, maximum_length=10) tac_dtl_id ;
    // decimal(&quot;\x01&quot;, 0, maximum_length=10) tac_id ;
    // string(&quot;\x01&quot;, 20) tac_name ;
    // decimal(&quot;\x01&quot;, 0, maximum_length=39) priority ;
    // string(&quot;\x01&quot;, 4000) target_rule = NULL ;
    // string(&quot;\x01&quot;, 4000) alt_rule = NULL ;
    // date(&quot;YYYYMMDD&quot;)('\x01') eff_dt ;
    // date(&quot;YYYYMMDD&quot;)('\x01') term_dt ;
    // string(1) newline = &quot;\n&quot; ;
    // xml_doc_t alt_xml ;
    // end ;</format><reject>type qual_priority_t =
    // record
    //   string(int) qual;
    //   decimal(&quot;&quot;) priority;
    // end;
    // constant string(int)[int] OVERRIDE_QUALIFIERS = [vector 'DESI_CD', 'DOSAGE_FORM', 'MSC', 'ROA', 'RXOTC' ];
    // constant qual_priority_t[int] QUAL_PRIORITY =[vector [record  qual &quot;NDC11&quot;        priority &quot;1&quot;],
    //                                                       [record qual &quot;NDC9&quot;         priority &quot;2&quot;],
    //                                                       [record qual &quot;NDC5&quot;         priority &quot;3&quot;],
    //                                                       [record qual &quot;GPI14&quot;        priority &quot;4&quot;],
    //                                                       [record qual &quot;GPI12&quot;        priority &quot;5&quot;],
    //                                                       [record qual &quot;GPI10&quot;        priority &quot;6&quot;],
    //                                                       [record qual &quot;DOSAGE_FORM&quot;  priority &quot;7&quot;],
    //                                                       [record qual &quot;ROA&quot;          priority &quot;8&quot;],
    //                                                       [record qual &quot;DRUG_NAME&quot;    priority &quot;9&quot;],
    //                                                       [record qual &quot;GPI8&quot;         priority &quot;10&quot;],
    //                                                       [record qual &quot;GPI6&quot;         priority &quot;11&quot;],
    //                                                       [record qual &quot;GPI4&quot;         priority &quot;12&quot;],
    //                                                       [record qual &quot;MSC&quot;          priority &quot;13&quot;],
    //                                                       [record qual &quot;RXOTC&quot;        priority &quot;14&quot;],
    //                                                       [record qual &quot;DAYS_UNTIL_DRUG_STATUS_INACTIVE&quot; priority &quot;15&quot;] ,
    //                                                       [record qual &quot;STATUS_CD&quot;  priority   &quot;16&quot;],
    //                                                       [record qual &quot;REPACKAGER&quot;   priority &quot;17&quot;],
    //                                                       [record qual &quot;DESI_CD&quot;    priority   &quot;18&quot;]];
    // type drug_data_set_dtl_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20) rxclaim_env_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) group = NULL(&quot;&quot;) ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) run_eff_dt = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) drug_data_set_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) drug_data_set_id ;
    //   string(&quot;\x01&quot;, maximum_length=11) ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=14) gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=1) status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=8) eff_dt = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=8) term_dt = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=8) inactive_dt = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=1) msc ;
    //   string(&quot;\x01&quot;, maximum_length=70) drug_name ;
    //   string(&quot;\x01&quot;, maximum_length=30) prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=3) rx_otc ;
    //   string(&quot;\x01&quot;, maximum_length=1) rx_otc_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1) desi ;
    //   string(&quot;\x01&quot;, maximum_length=2) roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4) dosage_form_cd ;
    //   decimal(&quot;\x01&quot;.5, maximum_length=15) prod_strength ;
    //   string(&quot;\x01&quot;, maximum_length=1) repack_cd ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi8_desc ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type user_defined_list_rule_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) user_defined_list_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) user_defined_list_name ;
    //   string(&quot;\x01&quot;, maximum_length=60) user_defined_list_desc ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) user_defined_list_rule_id ;
    //   string(&quot;\x01&quot;, maximum_length=4000) rule = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=1) incl_cd ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type rule_xwalk_t=
    // record
    //   decimal(&quot;\x01&quot;) udl_id;
    //   decimal(&quot;\x01&quot;) udl_rule_id;
    //   string(&quot;\x01&quot;) udl_nm;
    //   string(&quot;\x01&quot;) udl_desc;
    //   decimal(&quot;\x01&quot;) rule_priority;
    //   string(1) inclusion_cd;
    //   string(int) qualifier_cd;
    //   string(int) operator;
    //   string(int) compare_value;
    //   string(1) conjunction_cd;
    //   decimal(&quot;\x01&quot;) rule_expression_id;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type udl_mstr_xwalk_t=
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) udl_id;
    //   string(&quot;\x01&quot;, maximum_length=20) udl_nm;
    //   string(&quot;\x01&quot;, maximum_length=60) user_defined_list_desc;
    //   string(int)[int] qual_list;
    //   decimal(1) override_flg;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type product_lkp_t =
    // record
    //   int dl_bit;
    //   string(&quot;\x01&quot;, maximum_length=11)  ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=14)  gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=1)  status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=8) inactive_dt ;
    //   string(&quot;\x01&quot;, maximum_length=1)  msc ;
    //   string(&quot;\x01&quot;, maximum_length=70)  drug_name ;
    //   string(&quot;\x01&quot;, maximum_length=3)  rx_otc ;
    //   string(&quot;\x01&quot;, maximum_length=1)  desi ;
    //   string(&quot;\x01&quot;, maximum_length=2)  roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4)  dosage_form_cd ;
    //   decimal(&quot;\x01&quot;.5, maximum_length=15) prod_strength ;
    //   string(&quot;\x01&quot;, maximum_length=1)  repack_cd ;
    //   string(&quot;\x01&quot;, maximum_length=30) prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi8_desc ;
    //   string(1) newline = &quot;\n&quot;;
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
    //   string(&quot;\x01&quot;, maximum_length=20)  carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20)  account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20)  group = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=11)  ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=14)  gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=1)  status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=8) eff_dt = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=8) term_dt = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=8) inactive_dt ;
    //   string(&quot;\x01&quot;, maximum_length=1)  msc ;
    //   string(&quot;\x01&quot;, maximum_length=70)  drug_name ;
    //   string(&quot;\x01&quot;, maximum_length=3)  rx_otc ;
    //   string(&quot;\x01&quot;, maximum_length=1)  desi ;
    //   string(&quot;\x01&quot;, maximum_length=2)  roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4)  dosage_form_cd ;
    //   decimal(&quot;\x01&quot;.5, maximum_length=15) prod_strength ;
    //   string(&quot;\x01&quot;, maximum_length=1)  repack_cd ;
    //   string(&quot;\x01&quot;, maximum_length=30) prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi8_desc ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type cag_ovrrd_ref_file_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20)  carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20)  account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20)  group = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=1) is_future_snap = 0;
    //   string(&quot;\x01&quot;) data_path;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type udl_exp_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) udl_id;
    //   string(&quot;\x01&quot;, maximum_length=20) udl_nm;
    //   string(&quot;\x01&quot;, maximum_length=60) udl_desc;
    //   bit_vector_t products;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
    //   bit_vector_t[int] contents;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tal_dtl_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_name ;
    //   string(&quot;\x01&quot;, maximum_length=60) tal_desc ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tal_dtl_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=20) nested_tal_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;, 6, maximum_length=39) priority ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tal_assoc_dtl_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name ;
    //   string(&quot;\x01&quot;, maximum_length=60) tal_assoc_desc ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tal_assoc_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=20) target_udl_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) alt_udl_name = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) alt_rank = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) constituent_rank = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=2) constituent_group = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=1) constituent_reqd = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=30) shared_qual ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tac_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tar_name = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tac_dtl_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tac_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tac_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tac_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) priority ;
    //   string(&quot;\x01&quot;, maximum_length=4000) target_rule = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=4000) alt_rule = NULL(&quot;&quot;) ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tac_rule_xwalk_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tac_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tac_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) priority ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
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
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type contents_alt_st =
    // record
    //  bit_vector_t target_prdcts;
    //  bit_vector_t alternate_prdcts;
    //  string(&quot;\x01&quot;, maximum_length=40) tal_assoc_name;
    // end;
    // type tac_st_contents =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_id;
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
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tac_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tac_name ;
    //   tac_contents_t[int] tac_contents;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
    //   string(1) newline = '\n';
    // end;
    // type tal_container_xwalk_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20) tal_name ;
    //   int target_dl_bit;
    //   record
    //    int alt_prd;
    //    int alt_rank;
    //   end[int] alt_prdcts;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type target_udl_products_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20) target_udl_name = NULL(&quot;&quot;) ;
    //   bit_vector_t target_products;
    // end;
    // type alt_udl_products_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20) alt_udl_name = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) alt_rank = NULL(&quot;&quot;) ;
    //   bit_vector_t alt_products;
    // end;
    // type alt_exclusion_products_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tal_assoc_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tac_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tar_name = NULL(&quot;&quot;) ;
    //   bit_vector_t target_product_dtl;
    //   bit_vector_t alt_product_dtl;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tal_assoc_xwalk_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name ;
    //   string(&quot;\x01&quot;, maximum_length=30) shared_qual ;
    //   string(&quot;\x01&quot;, maximum_length=10) override_tac_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=10) override_tar_name = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tal_assoc_type_cd ;
    //   target_udl_products_t[int] target_udl_products;
    //   alt_udl_products_t[int] alt_udl_products;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type form_data_set_dtl_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) formulary_data_set_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_id = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=10) formulary_cd = &quot;&quot; ;
    //   string(&quot;\x01&quot;, maximum_length=20) carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) group = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) rxclaim_env_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) customer_name = NULL(&quot;&quot;);
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) last_exp_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) run_eff_dt = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) formulary_data_set_dtl_id ;
    //   string(&quot;\x01&quot;, maximum_length=11) ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=2) formulary_tier ;
    //   string(&quot;\x01&quot;, maximum_length=2) formulary_status ;
    //   string(&quot;\x01&quot;, maximum_length=1) pa_reqd_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) specialty_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) step_therapy_ind ;
    //   string(&quot;\x01&quot;, maximum_length=50) formulary_tier_desc ;
    //   string(&quot;\x01&quot;, maximum_length=50) formulary_status_desc ;
    //   string(&quot;\x01&quot;, maximum_length=1) pa_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1) step_therapy_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=100) step_therapy_group_name = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) step_therapy_step_number = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type form_ovrrd_xwalk_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_name ;
    //   string(&quot;\x01&quot;, maximum_length=10) formulary_cd = &quot;&quot; ;
    //   string(&quot;\x01&quot;, maximum_length=20)  carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20)  account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20)  group = NULL(&quot;&quot;) ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) last_exp_dt ;
    //   string(&quot;\x01&quot;, maximum_length=11) ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=2) formulary_tier ;
    //   string(&quot;\x01&quot;, maximum_length=2) formulary_status ;
    //   string(&quot;\x01&quot;, maximum_length=1) pa_reqd_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) specialty_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) step_therapy_ind ;
    //   string(&quot;\x01&quot;, maximum_length=50) formulary_tier_desc ;
    //   string(&quot;\x01&quot;, maximum_length=50) formulary_status_desc ;
    //   string(&quot;\x01&quot;, maximum_length=1) pa_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1) step_therapy_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=100) step_therapy_group_name = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) step_therapy_step_number = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type form_ovrrd_ref_file_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_name ;
    //   string(&quot;\x01&quot;, maximum_length=20)  carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20)  account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20)  group = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) customer_name = NULL(&quot;&quot;);
    //   decimal(&quot;\x01&quot;,0, maximum_length=1) is_future_snap = 0;
    //   string(&quot;\x01&quot;) data_path;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) run_eff_dt = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type output_profile_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) output_profile_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) rxclaim_env_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_id = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) output_profile_form_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) output_profile_job_dtl_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) output_profile_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) alias_name = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) alias_priority = NULL ;
    //   string(&quot;\x01&quot;, maximum_length=20) carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) group = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) tac_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) tar_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) tsd_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) job_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) job_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) customer_name = NULL(&quot;&quot;);
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) run_day = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) lob_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=4) run_jan1_start_mmdd = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=4) run_jan1_end_mmdd = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;) future_flg = 0;
    //   string(&quot;\x01&quot;, maximum_length=30) formulary_pseudonym = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) notes_id = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=60) output_profile_desc ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39 ) formulary_option_cd ;
    //   string(&quot;\x01&quot;, maximum_length=20) layout_name ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) as_of_dt = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=1) st_tac_ind ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type form_product_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=10) formulary_cd = &quot;&quot; ;
    //   string(&quot;\x01&quot;, maximum_length=11) ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=2) formulary_tier ;
    //   string(&quot;\x01&quot;, maximum_length=2) formulary_status ;
    //   string(&quot;\x01&quot;, maximum_length=1) pa_reqd_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) specialty_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) step_therapy_ind ;
    //   string(&quot;\x01&quot;, maximum_length=50) formulary_tier_desc ;
    //   string(&quot;\x01&quot;, maximum_length=50) formulary_status_desc ;
    //   string(&quot;\x01&quot;, maximum_length=1) pa_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1) step_therapy_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=100) step_therapy_group_name = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) step_therapy_step_number = NULL(&quot;&quot;) ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) last_exp_dt ;
    // end;
    // type cag_product_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=11) ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=14) gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=1) status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=8) eff_dt = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=8) term_dt = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=8) inactive_dt ;
    //   string(&quot;\x01&quot;, maximum_length=1) msc ;
    //   string(&quot;\x01&quot;, maximum_length=70) drug_name ;
    //   string(&quot;\x01&quot;, maximum_length=3) rx_otc ;
    //   string(&quot;\x01&quot;, maximum_length=1) desi ;
    //   string(&quot;\x01&quot;, maximum_length=2) roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4) dosage_form_cd ;
    //   decimal(&quot;\x01&quot;.5, maximum_length=15) prod_strength ;
    //   string(&quot;\x01&quot;, maximum_length=1) repack_cd ;
    //   string(&quot;\x01&quot;, maximum_length=30) prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi8_desc ;
    // end;
    // type cag_rlp_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20) carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) group = NULL(&quot;&quot;) ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) run_eff_dt = NULL(&quot;&quot;) ;
    //   decimal(1) cag_priority = 0;
    //   cag_product_t[int] prdcts;
    // end;
    // type form_rlp_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) group = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) customer_name = NULL(&quot;&quot;);
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) run_eff_dt = NULL(&quot;&quot;) ;
    //   decimal(1) cag_priority = 0;
    //   form_product_t[int] prdcts;
    // end;
    // type tsd_dtl_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tsd_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tsd_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tsd_name ;
    //   string(&quot;\x01&quot;, maximum_length=30) tsd_cd ;
    //   string(&quot;\x01&quot;, maximum_length=2) formulary_tier ;
    //   string(&quot;\x01&quot;, maximum_length=2) formulary_status ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) priority ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tsd_xwalk_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tsd_id ;
    //   string(&quot;\x01&quot;, maximum_length=30) tsd_cd ;
    //   bit_vector_t products;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type alt_constituent_prdct_t =
    // record
    //   bit_vector_t alt_prdcts;
    //   string(2) constituent_group = NULL(&quot;&quot;);
    //   string(1) constituent_reqd = NULL(&quot;&quot;);
    //   string(20) udl_nm = NULL(&quot;&quot;);
    // end;
    // type tal_container_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) tar_udl_nm = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=60) tal_desc ;
    //   decimal(&quot;\x01&quot;, 6, maximum_length=39) priority ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tal_assoc_type_cd ;
    //   bit_vector_t[int] target_prdcts;
    //   alt_constituent_prdct_t[int] alt_constituent_prdcts;
    //   string(&quot;\x01&quot;, maximum_length=30) shared_qual ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tac_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tar_name = NULL(&quot;&quot;) ;
    //   string(2)[int] constituent_grp_vec;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type master_cag_mapping_t =
    // record
    //   string(&quot;\x01&quot;) carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;) account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;) group = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;) future_flg = 'C';
    //   string(&quot;\x01&quot;) cag_override_data_path= NULL(&quot;&quot;);
    //   decimal(&quot;\x01&quot;)[int] qual_output_profile_ids ;
    //   record
    //     string(&quot;\x01&quot;) qual_output_profile_name ;
    //     string(&quot;\x01&quot;) rxclaim_env_name;
    //     decimal(&quot;\x01&quot;) [int] job_ids;
    //     string(&quot;\x01&quot;) [int] alias_names;
    //     string(&quot;\x01&quot;) [int] job_names;
    //     string(&quot;\x01&quot;) [int] formulary_names ;
    //     string(&quot;\x01&quot;) [int] form_override_data_paths;
    //     string(&quot;\x01&quot;, maximum_length=30)[int] formulary_pseudonyms;
    //     string(&quot;\x01&quot;, maximum_length=20) tal_name ;
    //     string(&quot;\x01&quot;) tac_name ;
    //     string(&quot;\x01&quot;) tar_name ;
    //     string(&quot;\x01&quot;) tsd_name ;
    //     string(&quot;\x01&quot;, maximum_length=1) st_tac_ind ;
    //   end[int] op_dtls;
    //   record
    //     decimal(&quot;\x01&quot;) non_qual_op_id;
    //     string(&quot;\x01&quot;) rxclaim_env_name;
    //     decimal(&quot;\x01&quot;) [int] job_ids;
    //     string(&quot;\x01&quot;)[int] formulary_names;
    //     string(&quot;\x01&quot;, maximum_length=30)[int] formulary_pseudonyms;
    //     string(&quot;\x01&quot;) [int] alias_names;
    //     string(&quot;\x01&quot;) [int] job_names;
    //   end[int] non_qual_output_profile_ids ;
    //   string(&quot;\x01&quot;) [int] err_msgs;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) as_of_dt = NULL(&quot;&quot;);
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tar_dtl_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tar_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tar_dtl_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tar_name ;
    //   string(&quot;\x01&quot;, maximum_length=1) sort_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) filter_ind ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) priority ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tar_dtl_type_cd ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tar_roa_df_set_id = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=4000) target_rule = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=4000) alt_rule = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=15) rebate_elig_cd = NULL(&quot;&quot;) ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tar_roa_df_set_dtl_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tar_roa_df_set_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tar_roa_df_set_id ;
    //   string(&quot;\x01&quot;, maximum_length=2) target_roa_cd = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=4) target_dosage_form_cd = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=2) alt_roa_cd = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=4) alt_dosage_form_cd = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) priority ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tar_rule_xwalk_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tar_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tar_dtl_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tar_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tar_roa_df_set_id = NULL(&quot;&quot;);
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tar_dtl_type_cd ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) priority ;
    //   string(&quot;\x01&quot;, maximum_length=1) sort_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) filter_ind ;
    //   string(&quot;\x01&quot;, maximum_length=15) rebate_elig_cd = NULL(&quot;&quot;) ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
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
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type alt_run_alt_dtl_load_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=16 ) alt_run_alt_dtl_id = -1 ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=16 ) alt_run_id = -1;
    //   decimal(&quot;\x01&quot;,0, maximum_length=16 ) alt_run_target_dtl_id = -1 ;
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_name ;
    //   string(&quot;\x01&quot;, maximum_length=11) target_ndc ;
    //   string(&quot;\x01&quot;, maximum_length=11) alt_ndc ;
    //   string(&quot;\x01&quot;, maximum_length=2) alt_formulary_tier ;
    //   string(&quot;\x01&quot;, maximum_length=1) alt_multi_src_cd ;
    //   string(&quot;\x01&quot;, maximum_length=2) alt_roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4) alt_dosage_form_cd ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39 ) rank ;
    //   string(&quot;\x01&quot;, maximum_length=1) alt_step_therapy_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) alt_pa_reqd_ind ;
    //   string(&quot;\x01&quot;, maximum_length=2) alt_formulary_status ;
    //   string(&quot;\x01&quot;, maximum_length=1) alt_specialty_ind ;
    //   string(&quot;\x01&quot;, maximum_length=14) alt_gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=70) alt_prod_name_ext ;
    //   string(&quot;\x01&quot;, maximum_length=30) alt_prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=30) alt_prod_short_desc_grp;
    //   string(&quot;\x01&quot;, maximum_length=60) alt_gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) alt_gpi8_desc ;
    //   string(&quot;\x01&quot;, maximum_length=50) alt_formulary_tier_desc ;
    //   string(&quot;\x01&quot;, maximum_length=50) alt_formulary_status_desc ;
    //   string(&quot;\x01&quot;, maximum_length=1) alt_pa_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1) alt_step_therapy_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1000) alt_step_therapy_group_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=100) alt_step_therapy_step_number = NULL(&quot;&quot;) ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) rec_crt_ts ;
    //   string(&quot;\x01&quot;, maximum_length=30) rec_crt_user_id ;
    //   string(&quot;\x01&quot;, maximum_length=15) rebate_elig_cd = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=15) tad_eligible_cd= NULL(&quot;&quot;);
    //   decimal(&quot;\x01&quot;.3, maximum_length=7) alt_qty_adj = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=40) tal_assoc_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=40) tala = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) alt_udl = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;.6, maximum_length=39) tal_assoc_rank ;
    //   string(&quot;\x01&quot;, maximum_length=2) constituent_group = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=1) constituent_reqd = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type alt_run_target_dtl_load_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=16 ) alt_run_target_dtl_id = -1 ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=16 ) alt_run_id  = -1 ;
    //   string(&quot;\x01&quot;, maximum_length=40) tal_assoc_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=40) tala = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) tar_udl = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_name ;
    //   string(&quot;\x01&quot;, maximum_length=11) target_ndc ;
    //   string(&quot;\x01&quot;, maximum_length=2) target_formulary_tier ;
    //   string(&quot;\x01&quot;, maximum_length=2) target_formulary_status ;
    //   string(&quot;\x01&quot;, maximum_length=1) target_pa_reqd_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) target_step_therapy_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) target_specialty_ind ;
    //   string(&quot;\x01&quot;, maximum_length=1) target_multi_src_cd ;
    //   string(&quot;\x01&quot;, maximum_length=2) target_roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4) target_dosage_form_cd ;
    //   string(&quot;\x01&quot;, maximum_length=14) target_gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=70) target_prod_name_ext ;
    //   string(&quot;\x01&quot;, maximum_length=30) target_prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) target_gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) target_gpi8_desc ;
    //   string(&quot;\x01&quot;, maximum_length=50) target_formulary_tier_desc ;
    //   string(&quot;\x01&quot;, maximum_length=50) target_formulary_status_desc ;
    //   string(&quot;\x01&quot;, maximum_length=1) target_pa_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1) target_step_therapy_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1000) target_step_therapy_group_name = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) target_step_therapy_step_num = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=10) formulary_cd = NULL(&quot;&quot;) ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) rec_crt_ts ;
    //   string(&quot;\x01&quot;, maximum_length=30) rec_crt_user_id ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) last_exp_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type alt_run_load_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=16) alt_run_id = -1 ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) output_profile_id ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) run_start_ts = NULL(&quot;&quot;) ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) run_complete_ts = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) alt_run_status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1) published_ind ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) rec_crt_ts ;
    //   string(&quot;\x01&quot;, maximum_length=30) rec_crt_user_id ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) rec_last_upd_ts ;
    //   string(&quot;\x01&quot;, maximum_length=30) rec_last_upd_user_id ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) run_eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) as_of_dt = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type alt_run_load_adhoc_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=16) alt_run_id = -1 ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) output_profile_id ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) run_start_ts = NULL(&quot;&quot;) ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) run_complete_ts = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) alt_run_status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1) published_ind ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) rec_crt_ts ;
    //   string(&quot;\x01&quot;, maximum_length=30) rec_crt_user_id ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) rec_last_upd_ts ;
    //   string(&quot;\x01&quot;, maximum_length=30) rec_last_upd_user_id ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) run_eff_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type alt_run_t =
    // record
    //   decimal(&quot;|&quot;) alt_run_id ;
    //   decimal(&quot;|&quot;) output_profile_id ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;|&quot;) run_start_ts = NULL(&quot;&quot;) ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;|&quot;) run_complete_ts = NULL(&quot;&quot;) ;
    //   decimal(&quot;|&quot;) alt_run_status_cd ;
    //   string(&quot;|&quot;) published_ind ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;|&quot;) rec_crt_ts ;
    //   string(&quot;|&quot;) rec_crt_user_id ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;|&quot;) rec_last_upd_ts ;
    //   string(&quot;|&quot;) rec_last_upd_user_id ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\n&quot;) run_eff_dt ;
    // end;
    // type alt_run_alt_proxy_dtl_load_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=16 ) alt_run_id = -1;
    //   decimal(&quot;\x01&quot;,0, maximum_length=16 ) alt_run_alt_dtl_id = -1 ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=16 ) alt_run_target_dtl_id = -1 ;
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_name ;
    //   string(&quot;\x01&quot;, maximum_length=11) target_ndc ;
    //   string(&quot;\x01&quot;, maximum_length=40) tal_assoc_name ;
    //   string(&quot;\x01&quot;, maximum_length=11) alt_proxy_ndc ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39 ) rank ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) rec_crt_ts ;
    //   string(&quot;\x01&quot;, maximum_length=30) rec_crt_user_id ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type file_load_cntl_load_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=50) user_id ;
    //   string(&quot;\x01&quot;, maximum_length=1) file_load_type_cd ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) component_type_cd = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) file_load_status_cd ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) file_load_start_ts = NULL(&quot;&quot;) ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) rec_crt_ts ;
    //   string(&quot;\x01&quot;, maximum_length=30) rec_crt_user_id ;
    //   string(&quot;\x01&quot;, maximum_length=1) published_ind ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=16) alt_run_id = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type file_load_ctl_dtl_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=16) file_load_cntl_id ;
    //   string(&quot;\x01&quot;, maximum_length=4000) component_ids = NULL(&quot;&quot;) ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) as_of_date ;
    //   string(&quot;\x01&quot;, maximum_length=20) rxclaim_env_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) group = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) component_type_cd = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=512) file_name_w = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=1) published_ind ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=16) alt_run_id = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=512) report_file_name = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type file_load_err_dtl_load_t =
    // record
    //   decimal(&quot;,&quot;,0, maximum_length=16) file_load_cntl_id ;
    //   string(&quot;,&quot;, maximum_length=1024) err_desc ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;,&quot;) rec_crt_ts ;
    //   string(&quot;,&quot;, maximum_length=30) rec_crt_user_id ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;,&quot;) rec_last_upd_ts ;
    //   string(&quot;\n&quot;, maximum_length=30) rec_last_upd_user_id ;
    // end;
    // type tal_assoc_prdcts_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tal_assoc_type_cd ;
    //   bit_vector_t[int] target_prdcts;
    //   bit_vector_t[int] alt_prdcts;
    //   string(&quot;\x01&quot;, maximum_length=30) shared_qual ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tac_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tar_name = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type part_exp_master_file_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=16) file_load_cntl_id ;
    //   string(&quot;\x01&quot;, maximum_length=1024) component_ids ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) as_of_date ;
    //   string(&quot;\x01&quot;, maximum_length=20) rxclaim_env_name = &quot;&quot; ;
    //   string(&quot;\x01&quot;, maximum_length=50) cag_in = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=50) cag_out = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39, sign_reserved) component_type_cd = NULL(&quot;&quot;) ;
    //   utf8 string(&quot;\x01&quot;, maximum_length=512) file_name_w = NULL(&quot;&quot;) ;
    //   utf8 string(&quot;\x01&quot;, maximum_length=1) published_ind ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type alt_rank_t =
    // record
    //   int alt_rank;
    //   int[int] alt_prdcts;
    //   string(2) constituent_group = NULL(&quot;&quot;);
    //   string(1) constituent_reqd = NULL(&quot;&quot;);
    //   string(20) udl_nm = NULL(&quot;&quot;);
    // end;
    // type rebate_elig_contents_t =
    // record
    //   string(&quot;\x01&quot;) rebate_elig_cd;
    //   int[int] rebate_elig_prdcts;
    // end;
    // type target_prdct_t =
    // record
    //   int target_dl_bit;
    //   decimal('\x01') bucket_index = 0;
    //   decimal('\x01') udl_index = 0;
    //   decimal('\x01') tac_index = 0;
    //   alt_rank_t[int] alt_prdcts_rank;
    //   string(&quot;\x01&quot;)has_alt;
    //   decimal(1) ST_flag = 0;
    // end;
    // type tal_assoc_adhoc_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name ;
    //   string(&quot;\x01&quot;, maximum_length=2000) clinical_indn_desc = NULL(&quot;&quot;);
    //   string(&quot;\x01&quot;, maximum_length=60) tal_assoc_desc ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tal_assoc_type_cd ;
    //   record
    //     string(&quot;\x01&quot;, maximum_length=20) udl_nm;
    //     string(&quot;\x01&quot;, maximum_length=60) udl_desc;
    //     bit_vector_t products;
    //   end[int] target_udl_info ;
    //   record
    //      string(&quot;\x01&quot;, maximum_length=20) udl_nm;
    //      string(&quot;\x01&quot;, maximum_length=60) udl_desc;
    //      bit_vector_t products;
    //      string(2) constituent_group = NULL(&quot;&quot;);
    //      string(1) constituent_reqd = NULL(&quot;&quot;);
    //      decimal(&quot;\x01&quot;,0, maximum_length=39) constituent_rank = NULL(&quot;&quot;);
    //   end[int] alt_udl_info;
    //   string(&quot;\x01&quot;, maximum_length=30) shared_qual ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tac_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tar_name = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tal_container_adhoc_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=2000) clinical_indn_desc = NULL(&quot;&quot;);
    //   string(&quot;\x01&quot;, maximum_length=60) tal_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) tal_assoc_desc ;
    //   decimal(&quot;\x01&quot;, 6, maximum_length=39) priority ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tal_assoc_type_cd ;
    //   record
    //       string(&quot;\x01&quot;, maximum_length=20) udl_nm;
    //       string(&quot;\x01&quot;, maximum_length=60) udl_desc;
    //       bit_vector_t products;
    //   end[int] target_prdcts;
    //   record
    //       string(&quot;\x01&quot;, maximum_length=20) udl_nm;
    //       string(&quot;\x01&quot;, maximum_length=60) udl_desc;
    //       bit_vector_t products;
    //       string(2) constituent_group = NULL(&quot;&quot;);
    //       string(1) constituent_reqd = NULL(&quot;&quot;);
    //       decimal(&quot;\x01&quot;,0, maximum_length=39) constituent_rank = NULL(&quot;&quot;);
    //   end[int] alt_prdcts;
    //   string(&quot;\x01&quot;, maximum_length=30) shared_qual ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tac_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tar_name = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tal_assoc_adhoc_enrich_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name ;
    //   string(&quot;\x01&quot;, maximum_length=2000) clinical_indn_desc = NULL(&quot;&quot;);
    //   string(&quot;\x01&quot;, maximum_length=60) tal_assoc_desc ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tal_assoc_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=20) udl_id = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=60) udl_desc;
    //   string(&quot;\x01&quot;, maximum_length=20) Target_Alternative;
    //   bit_vector_t products;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) alt_rank = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=30) shared_qual ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tac_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tar_name = NULL(&quot;&quot;) ;
    //   string(2) constituent_group = NULL(&quot;&quot;);
    //   string(1) constituent_reqd = NULL(&quot;&quot;);
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) constituent_rank = NULL(&quot;&quot;);
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tal_container_adhoc_enrich_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20) tal_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_id = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=2000) clinical_indn_desc = NULL(&quot;&quot;);
    //   string(&quot;\x01&quot;, maximum_length=60) tal_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) tal_assoc_desc ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tal_assoc_type_cd ;
    //   decimal(&quot;\x01&quot;, 6, maximum_length=39) tal_assoc_rank = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) udl_id = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=60) udl_desc;
    //   string(&quot;\x01&quot;, maximum_length=20) Target_Alternative;
    //   bit_vector_t products;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) alt_rank = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=30) shared_qual ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tac_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tar_name = NULL(&quot;&quot;) ;
    //   string(2) constituent_group = NULL(&quot;&quot;);
    //   string(1) constituent_reqd = NULL(&quot;&quot;);
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) constituent_rank = NULL(&quot;&quot;);
    //   string(1) newline = &quot;\n&quot;;
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
    //   decimal(&quot;\x01&quot;,0, maximum_length=16 ) alt_run_target_dtl_id ;
    //   string(&quot;\x01&quot;, maximum_length=40) tal_assoc_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_name ;
    //   string(&quot;\x01&quot;, maximum_length=11) target_ndc ;
    // end;
    // type alt_run_alt_dtl_ref_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=16 ) alt_run_alt_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=16 ) alt_run_target_dtl_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_name ;
    //   string(&quot;\x01&quot;, maximum_length=40) tal_assoc_name ;
    //   string(&quot;\x01&quot;, maximum_length=11) target_ndc ;
    //   string(&quot;\x01&quot;, maximum_length=11) alt_ndc ;
    // end;
    // type master_cag_mapping_adhoc_t =
    // record
    //   string(&quot;\x01&quot;) carrier = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;) account = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;) group = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;) cag_override_data_path= NULL(&quot;&quot;);
    //   decimal(&quot;\x01&quot;)[int] output_profile_id ;
    //   record
    //     string(&quot;\x01&quot;) output_profile_name ;
    //     string(&quot;\x01&quot;) [int] alias_names;
    //     string(&quot;\x01&quot;) [int] job_names;
    //     string(&quot;\x01&quot;)[int] formulary_names ;
    //     string(&quot;\x01&quot;) [int] form_override_data_paths;
    //     string(&quot;\x01&quot;)[int] formulary_pseudonyms ;
    //     string(&quot;\x01&quot;) tal_name ;
    //     string(&quot;\x01&quot;) tac_name ;
    //     string(&quot;\x01&quot;) tar_name ;
    //     string(&quot;\x01&quot;) tsd_name ;
    //     string(&quot;\x01&quot;, maximum_length=1) st_tac_ind ;
    //   end[int] op_dtls;
    //   string(&quot;\x01&quot;)[int] err_msgs ;
    //   string(1) newline = &quot;\n&quot;;
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
    //   decimal(&quot;\x01&quot;,0, maximum_length=16) alt_run_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=16) job_run_id ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) rec_crt_ts ;
    //   string(&quot;\x01&quot;, maximum_length=30) rec_crt_user_id ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type alias_dtl_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) alias_dtl_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) alias_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) alias_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) qual_priority = 99;
    //   string(&quot;\x01&quot;, maximum_length=10) qual_id_type_cd = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=14) qual_id_value = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=70) search_txt ;
    //   string(&quot;\x01&quot;, maximum_length=70) replace_txt = NULL(&quot;&quot;) ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) rank ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type alias_info_t =  record
    //         decimal(&quot;\x01&quot;,0, maximum_length=39) qual_priority = 99;
    //         string(&quot;\x01&quot;, maximum_length=14) qual_id_value = NULL(&quot;&quot;) ;
    //         string(&quot;\x01&quot;, maximum_length=70) search_txt ;
    //         string(&quot;\x01&quot;, maximum_length=70) replace_txt  ;
    //         date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) eff_dt;
    //         date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) term_dt;
    // end;
    // type alias_xwalk_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) alias_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) alias_name ;
    //   alias_info_t[int] alias_info;
    // end;
    // type target_alt_rank_t =
    // record
    //     int target_dl_bit;
    //     decimal('\x01') bucket_index = 0;
    //     decimal('\x01') udl_index = 0;
    //     string(20) tar_udl_nm =NULL(&quot;&quot;);
    //     decimal('\x01') tac_index = 0;
    //     record
    //       int alt_rank;
    //       int[int] alt_prdcts;
    //       string(2) constituent_group = NULL(&quot;&quot;);
    //       string(1) constituent_reqd = NULL(&quot;&quot;);
    //       string(20) udl_nm =NULL(&quot;&quot;);
    //     end[int] alt_prdcts_rank;
    //     string(&quot;\x01&quot;)has_alt;
    //     decimal(1) ST_flag =0;
    // end;
    // type alias_content_xwalk_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=13) alias_set_name;
    //   string(&quot;\x01&quot;, maximum_length=11)  ndc11 ;
    //   string(&quot;\x01&quot;) alias_label_nm ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type gpi_rank_ratio_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=14) gpi14 ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) rank ;
    //   decimal(&quot;\x01&quot;.3, maximum_length=6) ratio = NULL(&quot;&quot;) ;
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) run_eff_dt = NULL(&quot;&quot;) ;
    //  end;
    // type tad_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tad_id ;
    //   string(&quot;\x01&quot;, maximum_length=14) target_gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=12) alt_grouping_gpi12 ;
    //   string(&quot;\x01&quot;, maximum_length=14) alt_selection_id ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) rank ;
    //   decimal(&quot;\x01&quot;.3, maximum_length=7) qty_adjust_factor = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tad_xwalk_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=14) target_gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=6) alt_selection_cd;
    //   string(&quot;\x01&quot;, maximum_length=14)[int] alt_selection_ids ;
    //   record
    //     decimal(&quot;\x01&quot;,0, maximum_length=39) rank ;
    //     decimal(&quot;\x01&quot;.3, maximum_length=7) qty_adjust_factor = NULL(&quot;&quot;) ;
    //   end[int] tad_alt_dtls ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type step_grp_xwalk_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=11) ndc11 ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) step_therapy_step_number = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=100)[int] step_therapy_group_names  ;
    // end;
    // type step_grp_num_xwalk_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tac_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tac_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) priority ;
    //   decimal(&quot;\x01&quot;) _target_st_grp_num = allocate();
    //   decimal(&quot;\x01&quot;)[int] _alt_st_grp_nums = allocate();
    // end;
    // type tal_assoc_clinical_indn_t =
    // record
    //  decimal(&quot;\x01&quot;,0, maximum_length=10) tal_assoc_id ;
    //  string(&quot;\x01&quot;, maximum_length=40) tal_assoc_name ;
    //  decimal(&quot;\x01&quot;,0, maximum_length=10) clinical_indn_id = NULL(&quot;&quot;);
    //  string(&quot;\x01&quot;, maximum_length=20) clinical_indn_name ;
    //  string(&quot;\x01&quot;, maximum_length=200) clinical_indn_desc = NULL(&quot;&quot;);
    //  decimal(&quot;\x01&quot;,0, maximum_length=39) rank = NULL(&quot;&quot;);
    // end;
    // type alt_run_clinical_indn_dtl_load_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=16) alt_run_id = -1 ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=16) alt_run_target_dtl_id = -1 ;
    //   string(&quot;\x01&quot;, maximum_length=20) formulary_name ;
    //   string(&quot;\x01&quot;, maximum_length=11) target_ndc ;
    //   string(&quot;\x01&quot;, maximum_length=40) tal_assoc_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) rank ;
    //   datetime(&quot;YYYY-MM-DD HH24:MI:SS&quot;)(&quot;\x01&quot;) rec_crt_ts ;
    //   string(&quot;\x01&quot;, maximum_length=30) rec_crt_user_id ;
    //   string(&quot;\x01&quot;, maximum_length=200) clinical_indn_desc = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type form_target_reject_t =
    // record
    // string(&quot;,&quot;) error_msg;
    // decimal(&quot;,&quot;) tal_id ;
    // string(&quot;,&quot;) tal_name ;
    // string(&quot;,&quot;) tal_assoc_name = NULL(&quot;&quot;) ;
    // string(&quot;,&quot;) ndc11 = NULL(&quot;&quot;);
    // string(&quot;,&quot;) gpi14 = NULL(&quot;&quot;);
    // string(&quot;\n&quot;) prod_short_desc = NULL(&quot;&quot;);
    // end;
    // type form_alt_reject_t =
    // record
    // string(&quot;,&quot;) error_msg;
    // decimal(&quot;,&quot;) tal_id ;
    // string(&quot;,&quot;) tal_name ;
    // string(&quot;,&quot;) tal_assoc_name = NULL(&quot;&quot;) ;
    // string(&quot;,&quot;) target_ndc11 = NULL(&quot;&quot;);
    // string(&quot;,&quot;) alt_ndc11 = NULL(&quot;&quot;);
    // string(&quot;,&quot;) gpi14 = NULL(&quot;&quot;);
    // string(&quot;\n&quot;) prod_short_desc = NULL(&quot;&quot;);
    // end;
    // type tal_assoc_enriched_data_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) tal_assoc_type_cd ;
    //   string(&quot;\x01&quot;, maximum_length=20) Target_Alternative;
    //   string(&quot;\x01&quot;, maximum_length=11)  ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=14)  gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=1)  msc ;
    //   string(&quot;\x01&quot;, maximum_length=70)  drug_name ;
    //   string(&quot;\x01&quot;, maximum_length=30) prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=2)  roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4)  dosage_form_cd ;
    //   string(&quot;\x01&quot;, maximum_length=3)  rx_otc ;
    //   string(&quot;\x01&quot;, maximum_length=1)  repack_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1)  status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=8) inactive_dt ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) alt_rank = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=30) shared_qual ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tac_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tar_name = NULL(&quot;&quot;) ;
    //   string(2) constituent_group = NULL(&quot;&quot;);
    //   string(1) constituent_reqd = NULL(&quot;&quot;);
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) constituent_rank = NULL(&quot;&quot;);
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tsd_enriched_data_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tsd_id ;
    //   string(&quot;\x01&quot;, maximum_length=30) tsd_cd ;
    //   string(&quot;\x01&quot;, maximum_length=11)  ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=14)  gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=1)  msc ;
    //   string(&quot;\x01&quot;, maximum_length=70)  drug_name ;
    //   string(&quot;\x01&quot;, maximum_length=30) prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=2)  roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4)  dosage_form_cd ;
    //   string(&quot;\x01&quot;, maximum_length=3)  rx_otc ;
    //   string(&quot;\x01&quot;, maximum_length=1)  repack_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1)  status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=8) inactive_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tac_enriched_data_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tac_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tac_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=39) priority ;
    //   string(&quot;\x01&quot;, maximum_length=20) Target_Alternative;
    //   string(&quot;\x01&quot;, maximum_length=11)  ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=14)  gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=1)  msc ;
    //   string(&quot;\x01&quot;, maximum_length=70)  drug_name ;
    //   string(&quot;\x01&quot;, maximum_length=30) prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=2)  roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4)  dosage_form_cd ;
    //   string(&quot;\x01&quot;, maximum_length=3)  rx_otc ;
    //   string(&quot;\x01&quot;, maximum_length=1)  repack_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1)  status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=8) inactive_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type tar_enriched_data_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tar_id  ;
    //   string(&quot;\x01&quot;, maximum_length=20) tar_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) Target_Alternative;
    //   string(&quot;\x01&quot;, maximum_length=11)  ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=14)  gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=1)  msc ;
    //   string(&quot;\x01&quot;, maximum_length=70)  drug_name ;
    //   string(&quot;\x01&quot;, maximum_length=30) prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=2)  roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4)  dosage_form_cd ;
    //   string(&quot;\x01&quot;, maximum_length=3)  rx_otc ;
    //   string(&quot;\x01&quot;, maximum_length=1)  repack_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1)  status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=8) inactive_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type asso_enriched_tac_data_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) Target_Alternative;
    //   string(&quot;\x01&quot;, maximum_length=11)  ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=14)  gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=1)  msc ;
    //   string(&quot;\x01&quot;, maximum_length=70)  drug_name ;
    //   string(&quot;\x01&quot;, maximum_length=30) prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=2)  roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4)  dosage_form_cd ;
    //   string(&quot;\x01&quot;, maximum_length=3)  rx_otc ;
    //   string(&quot;\x01&quot;, maximum_length=1)  repack_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1)  status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=8) inactive_dt ;
    //   string(&quot;\x01&quot;, maximum_length=30) shared_qual ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tac_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=20) override_tar_name = NULL(&quot;&quot;) ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type asso_enriched_sorted_data_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) tal_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_name ;
    //   string(&quot;\x01&quot;, maximum_length=20) tal_assoc_name = NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=11)  Target_ndc11 ='N/A';
    //   string(&quot;\x01&quot;, maximum_length=20) Target_Alternative;
    //   string(&quot;\x01&quot;, maximum_length=11)  ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=15) tad_eli_code= NULL(&quot;&quot;) ;
    //   string(&quot;\x01&quot;, maximum_length=14)  gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=1)  msc ;
    //   string(&quot;\x01&quot;, maximum_length=70)  drug_name ;
    //   string(&quot;\x01&quot;, maximum_length=30) prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=2)  roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4)  dosage_form_cd ;
    //   string(&quot;\x01&quot;, maximum_length=3)  rx_otc ;
    //   string(&quot;\x01&quot;, maximum_length=1)  repack_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1)  status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=8) inactive_dt ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // type asso_enriched_diff_data_t =
    // record
    //   string(&quot;\x01&quot;, maximum_length=20) Target_Alternative;
    //   string(&quot;\x01&quot;, maximum_length=11)  ndc11 ;
    //   string(&quot;\x01&quot;, maximum_length=14)  gpi14 ;
    //   string(&quot;\x01&quot;, maximum_length=1)  msc ;
    //   string(&quot;\x01&quot;, maximum_length=70)  drug_name ;
    //   string(&quot;\x01&quot;, maximum_length=30) prod_short_desc ;
    //   string(&quot;\x01&quot;, maximum_length=60) gpi14_desc ;
    //   string(&quot;\x01&quot;, maximum_length=2)  roa_cd ;
    //   string(&quot;\x01&quot;, maximum_length=4)  dosage_form_cd ;
    //   string(&quot;\x01&quot;, maximum_length=3)  rx_otc ;
    //   string(&quot;\x01&quot;, maximum_length=1)  repack_cd ;
    //   string(&quot;\x01&quot;, maximum_length=1)  status_cd ;
    //   string(&quot;\x01&quot;, maximum_length=8) inactive_dt ;
    //   end;
    // type udl_ref_t =
    // record
    //   date(&quot;YYYYMMDD&quot;)(&quot;\x01&quot;) as_of_dt ;
    // end;
    // type output_profile_rebate_dtl_t =
    // record
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) output_profile_rebate_dtl_id ;
    //   string(&quot;\x01&quot;, maximum_length=20) udl_name ;
    //   decimal(&quot;\x01&quot;,0, maximum_length=10) output_profile_id ;
    //   string(&quot;\x01&quot;, maximum_length=15) rebate_elig_cd ;
    //   string(1) newline = &quot;\n&quot;;
    // end;
    // metadata type = tac_dtl_t ;</reject><error>type error_info_V2_16 = record
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
    // type adc_error_info_t = record
    //   error_info_t error_info;
    // end;
    // type adc_error_info_vector_t = adc_error_info_t[big endian integer(4)];
    // metadata type = error_info_t ;</error></out0></out-port>
    //
    //
    //
    //
    //
    //
    //       <count>1</count><vector_length_prefix_type0>&quot;big endian integer(4)&quot;</vector_length_prefix_type0><character_set0>utf-8</character_set0><dataset_analysis>eme_dataset_mapping(file)=xread,r</dataset_analysis><source_files>/home/pdalal11/rule_test.xml</source_files><always_stream_input>false</always_stream_input><condition_interpretation>Replace with flow</condition_interpretation><normalize>value normalization-strategy Full full value normalization-strategy Base* base-elements-only default dollar_substitution full</normalize><vector_based_subkeys>true</vector_based_subkeys><parser>expat</parser><check_is_valid>false</check_is_valid><ramp>0.0</ramp><subgraph></subgraph><ignore_namespaces>true</ignore_namespaces><normalization_strategy>Full</normalization_strategy><missing_fields>ignore</missing_fields><unknown_elements>ignore</unknown_elements><selected_items0>True /Rule/Rule/Qual /Rule/Rule/OR /Rule/Rule /Rule/AND /Rule/Qual /Rule /Rule/Rule/Qual/\@type /Rule/Rule/Qual/\@op /Rule/Qual/\@type /Rule/Qual/\@op /Rule/Rule/Qual/text() /Rule/Rule/OR/text() /Rule/AND/text() /Rule/Qual/text()</selected_items0><unknown_attributes>ignore</unknown_attributes><generate_ID_fields>true</generate_ID_fields><xml_doc_field_name>alt_rule</xml_doc_field_name><reject_port_version>2.11:</reject_port_version><reset_sequence_numbers>checkpoint</reset_sequence_numbers><limit>0</limit><variable1_analysis>optional input_type xread &lt;parameter eme_dataset_metadata&gt;</variable1_analysis><reject_only_when_unanimous>false</reject_only_when_unanimous><cardinality_constraints>ignore</cardinality_constraints>
    //
    //     </component>
    //
    val out = in
    out
  }

}
