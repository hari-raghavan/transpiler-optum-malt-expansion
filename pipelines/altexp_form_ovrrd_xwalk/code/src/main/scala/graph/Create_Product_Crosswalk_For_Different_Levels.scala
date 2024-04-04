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

object Create_Product_Crosswalk_For_Different_Levels {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    import _root_.io.prophecy.abinitio.ScalaFunctions._
    def op_fl_nn_condition() = when(
        ((isnull(col("carrier")).and(isnull(col("account")))).and(isnull(col("group")))).and(isnull(col("run_eff_dt"))),
        concat(
          lit(Config.AI_SERIAL_HOME),
          lit("/deliver/."),
          lit("/"),
          lit(Config.OUTPUT_FILE_PREFIX),
          lit("form.C."),
          lit(Config.ENV_NM),
          lit("."),
          col("customer_name"),
          lit("."),
          regexp_replace(col("formulary_name"), lit(" +"), lit("-")),
          lit(".NULL_BASE_LINE."),
          lit(Config.BUSINESS_DATE),
          lit(".dat")
        )
      ).when(
          ((isnull(col("carrier")).and(isnull(col("account")))).and(isnull(col("group")))).and(!isnull(col("run_eff_dt"))),
          concat(
            lit(Config.AI_SERIAL_HOME),
            lit("/deliver/."),
            lit("/"),
            lit(Config.OUTPUT_FILE_PREFIX),
            lit("form.F."),
            lit(Config.ENV_NM),
            lit("."),
            col("customer_name"),
            lit("."),
            regexp_replace(col("formulary_name"), lit(" +"), lit("-")),
            lit(".NULL_BASE_LINE."),
            lit(Config.BUSINESS_DATE),
            lit(".dat")
          )
        )
        .when(
          (col("carrier") === lit("*ALL")).and(isnull(col("run_eff_dt"))),
          concat(
            lit(Config.AI_SERIAL_HOME),
            lit("/deliver/."),
            lit("/"),
            lit(Config.OUTPUT_FILE_PREFIX),
            lit("form.C."),
            lit(Config.ENV_NM),
            lit("."),
            col("customer_name"),
            lit("."),
            regexp_replace(col("formulary_name"), lit(" +"), lit("-")),
            lit(".ALL_ALL_ALL."),
            lit(Config.BUSINESS_DATE),
            lit(".dat")
          )
        )
        .when(
          (col("carrier") === lit("*ALL")).and(!isnull(col("run_eff_dt"))),
          concat(
            lit(Config.AI_SERIAL_HOME),
            lit("/deliver/."),
            lit("/"),
            lit(Config.OUTPUT_FILE_PREFIX),
            lit("form.F."),
            lit(Config.ENV_NM),
            lit("."),
            col("customer_name"),
            lit("."),
            regexp_replace(col("formulary_name"), lit(" +"), lit("-")),
            lit(".ALL_ALL_ALL."),
            lit(Config.BUSINESS_DATE),
            lit(".dat")
          )
        )
        .when(
          (((!isnull(col("carrier")))
            .and(col("account") === lit("*ALL")))
            .and(col("group") === lit("*ALL")))
            .and(isnull(col("run_eff_dt"))),
          concat(
            lit(Config.AI_SERIAL_HOME),
            lit("/deliver/."),
            lit("/"),
            lit(Config.OUTPUT_FILE_PREFIX),
            lit("form.C."),
            lit(Config.ENV_NM),
            lit("."),
            col("customer_name"),
            lit("."),
            regexp_replace(col("formulary_name"), lit(" +"), lit("-")),
            lit("."),
            col("carrier"),
            lit("_ALL_ALL."),
            lit(Config.BUSINESS_DATE),
            lit(".dat")
          )
        )
        .when(
          (((!isnull(col("carrier")))
            .and(col("account") === lit("*ALL")))
            .and(col("group") === lit("*ALL")))
            .and(!isnull(col("run_eff_dt"))),
          concat(
            lit(Config.AI_SERIAL_HOME),
            lit("/deliver/."),
            lit("/"),
            lit(Config.OUTPUT_FILE_PREFIX),
            lit("form.F."),
            lit(Config.ENV_NM),
            lit("."),
            col("customer_name"),
            lit("."),
            regexp_replace(col("formulary_name"), lit(" +"), lit("-")),
            lit("."),
            col("carrier"),
            lit("_ALL_ALL."),
            lit(Config.BUSINESS_DATE),
            lit(".dat")
          )
        )
        .when(
          (((!isnull(col("carrier")))
            .and(!isnull(col("account"))))
            .and(col("group") === lit("*ALL")))
            .and(isnull(col("run_eff_dt"))),
          concat(
            lit(Config.AI_SERIAL_HOME),
            lit("/deliver/."),
            lit("/"),
            lit(Config.OUTPUT_FILE_PREFIX),
            lit("form.C."),
            lit(Config.ENV_NM),
            lit("."),
            col("customer_name"),
            lit("."),
            regexp_replace(col("formulary_name"), lit(" +"), lit("-")),
            lit("."),
            col("carrier"),
            lit("_"),
            col("account"),
            lit("_ALL."),
            lit(Config.BUSINESS_DATE),
            lit(".dat")
          )
        )
        .when(
          (((!isnull(col("carrier")))
            .and(!isnull(col("account"))))
            .and(col("group") === lit("*ALL")))
            .and(!isnull(col("run_eff_dt"))),
          concat(
            lit(Config.AI_SERIAL_HOME),
            lit("/deliver/."),
            lit("/"),
            lit(Config.OUTPUT_FILE_PREFIX),
            lit("form.F."),
            lit(Config.ENV_NM),
            lit("."),
            col("customer_name"),
            lit("."),
            regexp_replace(col("formulary_name"), lit(" +"), lit("-")),
            lit("."),
            col("carrier"),
            lit("_"),
            col("account"),
            lit("_ALL."),
            lit(Config.BUSINESS_DATE),
            lit(".dat")
          )
        )
        .when(
          isnull(col("run_eff_dt")),
          concat(
            lit(Config.AI_SERIAL_HOME),
            lit("/deliver/."),
            lit("/"),
            lit(Config.OUTPUT_FILE_PREFIX),
            lit("form.C."),
            lit(Config.ENV_NM),
            lit("."),
            col("customer_name"),
            lit("."),
            regexp_replace(col("formulary_name"), lit(" +"), lit("-")),
            lit("."),
            col("carrier"),
            lit("_"),
            col("account"),
            lit("_"),
            col("group"),
            lit("."),
            lit(Config.BUSINESS_DATE),
            lit(".dat")
          )
        )
        .otherwise(
          concat(
            lit(Config.AI_SERIAL_HOME),
            lit("/deliver/."),
            lit("/"),
            lit(Config.OUTPUT_FILE_PREFIX),
            lit("form.F."),
            lit(Config.ENV_NM),
            lit("."),
            col("customer_name"),
            lit("."),
            regexp_replace(col("formulary_name"), lit(" +"), lit("-")),
            lit("."),
            col("carrier"),
            lit("_"),
            col("account"),
            lit("_"),
            col("group"),
            lit("."),
            lit(Config.BUSINESS_DATE),
            lit(".dat")
          )
        )
    
    val process_udf = udf({ (input: Seq[Row]) =>
      val outputRows = scala.collection.mutable.ArrayBuffer[Row]()
    
      var baseline_prdcts = Array[Row]();
      var tarall_prdcts = Array[Row]();
      var c_prdcts = Array[Row]();
      var ca_prdcts = Array[Row]();
      var prdcts = Array[Row]();
      var starall_prdcts = Array[Row]();
      var _products = Array[Row]();
    
      input.sortBy(r => (r.getAs[String]("formulary_name"), r.getAs[Int]("customer_name"), r.getAs[String]("run_eff_dt"), r.getAs[Int]("cag_priority") )).foreach { in =>
           val cag_priority = in.getAs[Int]("cag_priority")
           if (cag_priority == 1) {
              baseline_prdcts = in.getAs[Array[Row]]("prdcts")
              prdcts = baseline_prdcts
            } else if (cag_priority == 2) {
              starall_prdcts = Array.concat(baseline_prdcts.filter { x =>
                                              in.getAs[Array[Row]]("prdcts").filter(y => y.getAs[String]("ndc11") == x.getAs[String](1)).isEmpty
                                            }.distinct,
                                            in.getAs[Array[Row]]("prdcts")
              )
              prdcts = starall_prdcts
            } else if (cag_priority == 3) {
              prdcts =
                if (compareTo(starall_prdcts.length, 0) > 0)
                  Array.concat(starall_prdcts.filter(x => in.getAs[Array[Row]]("prdcts").filter(y => y.getAs[String]("ndc11") == x.getAs[String](1)).isEmpty).distinct,
                               in.getAs[Array[Row]]("prdcts")
                  )
                else
                  Array.concat(baseline_prdcts.filter(x => in.getAs[Array[Row]]("prdcts").filter(y => y.getAs[String]("ndc11") == x.getAs[String](1)).isEmpty).distinct,
                               in.getAs[Array[Row]]("prdcts")
                  )
              c_prdcts = Array.concat(c_prdcts, Array.fill(1)(Row(in.getAs[String]("carrier"), in.getAs[Array[Row]]("prdcts"))))
            } else if (cag_priority == 4) {
              prdcts = if (compareTo(c_prdcts.length, 0) > 0) {
                if (
                  (try {
                    (c_prdcts
                      .filter(xx => xx.getAs[String](0) == in.getAs[String]("carrier"))
                      .toArray).head
                      .getAs[Array[Row]](1)
                  } catch {
                    case error: Throwable => null
                  }) != null
                )
                  (c_prdcts
                    .filter(xx => xx.getAs[String](0) == in.getAs[String]("carrier"))
                    .toArray).head
                    .getAs[Array[Row]](1)
                else
                  Array[Row]()
              } else
                Array[Row]()
              _products = Array.concat(
                prdcts.filter(x => in.getAs[Array[Row]]("prdcts").filter(y => y.getAs[String]("ndc11") == x.getAs[String](1)).isEmpty).distinct,
                in.getAs[Array[Row]]("prdcts")
              )
              prdcts = if (compareTo(starall_prdcts.length, 0) > 0) {
                Array.concat(
                  starall_prdcts.filter {
                    x =>
                       _products.filter(y => y.getAs[String](1) == x.getAs[String](1)).isEmpty
                  }.distinct,
                  _products
                )
              } else {
                Array.concat(
                  baseline_prdcts.filter {
                    x =>
                       _products.filter(y => y.getAs[String](1) == x.getAs[String](1)).isEmpty
                  }.distinct,
                  _products
                )
              }
              ca_prdcts = Array.concat(
                ca_prdcts,
                Array.fill(1)(
                  Row(
                    in.getAs[String]("carrier"),
                    in.getAs[String]("account"),
                    _products
                  )
                )
              )
            } else if (cag_priority == 5) {
              prdcts = if (compareTo(ca_prdcts.length, 0) > 0) {
                if (
                  (try {
                    (ca_prdcts
                      .filter { xx =>
                        (xx.getAs[String](0) == in.getAs[String]("carrier")) && (xx.getAs[String](1) == in.getAs[String]("account"))
                      }
                      .toArray).head
                      .getAs[Array[Row]](2)
                  } catch {
                    case error: Throwable => null
                  }) != null
                )
                  (ca_prdcts
                    .filter { xx =>
                      (xx.getAs[String](0) == in.getAs[String]("carrier")) && (xx.getAs[String](1) == in.getAs[String]("account"))
                    }
                    .toArray).head
                    .getAs[Array[Row]](2)
                else
                  Array[Row]()
              } else
                Array[Row]()
              if ((prdcts.length == 0) && (compareTo(c_prdcts.length, 0) > 0)) {
                prdcts =
                  if (
                    (try {
                      (c_prdcts
                        .filter(xx => xx.getAs[String](0) == in.getAs[String]("carrier"))
                        .toArray).head
                        .getAs[Array[Row]](1)
                    } catch {
                      case error: Throwable => null
                    }) != null
                  )
                    (c_prdcts
                      .filter(xx => xx.getAs[String](0) == in.getAs[String]("carrier"))
                      .toArray).head
                      .getAs[Array[Row]](1)
                  else
                    Array[Row]()
              }
              _products = Array.concat(
                prdcts.filter(x => in.getAs[Array[Row]]("prdcts").filter(y => y.getAs[String]("ndc11") == x.getAs[String](1)).isEmpty).distinct,
                in.getAs[Array[Row]]("prdcts")
              )
              prdcts = if (compareTo(starall_prdcts.length, 0) > 0) {
                Array.concat(
                  starall_prdcts.filter {
                    x =>
                       _products.filter(y => y.getAs[String](1) == x.getAs[String](1)).isEmpty
                  }.distinct,
                  _products
                )
              } else {
                Array.concat(
                  baseline_prdcts.filter {
                    x =>
                       _products.filter(y => y.getAs[String](1) == x.getAs[String](1)).isEmpty
                  }.distinct,
                  _products
                )
              }
            }
          outputRows.append(Row(
            in.getAs[String]("formulary_name"),
            in.getAs[String]("carrier"),
            in.getAs[String]("account"),
            in.getAs[String]("group"),
            in.getAs[String]("customer_name"),
            in.getAs[String]("run_eff_dt"),
            in.getAs[Int]("cag_priority"),
            prdcts,
            in.getAs[String]("data_path")
          ))
      }
      outputRows
    }, 
    ArrayType(
        StructType(List(
          StructField("formulary_name", StringType, false),
          StructField("carrier", StringType, false),
          StructField("account", StringType, false),
          StructField("group", StringType, false),
          StructField("customer_name", StringType, false),
          StructField("run_eff_dt", StringType, false),
          StructField("cag_priority", IntegerType, false),
          StructField(
            "prdcts",
            ArrayType(
              StructType(
                StructField("formulary_cd", StringType, false),
                StructField("ndc11", StringType, false),
                StructField("formulary_tier", StringType, false),
                StructField("formulary_status", StringType, false),
                StructField("pa_reqd_ind", StringType, false),
                StructField("specialty_ind", StringType, false),
                StructField("step_therapy_ind", StringType, false),
                StructField("formulary_tier_desc", StringType, false),
                StructField("formulary_status_desc", StringType, false),
                StructField("pa_type_cd", StringType, false),
                StructField("step_therapy_type_cd", StringType, false),
                StructField("step_therapy_group_name", StringType, false),
                StructField("step_therapy_step_number", StringType, false),
                StructField("last_exp_dt", StringType, false)
              ),
              true
            ),
            true
          ),
          StructField("data_path", StringType, false)
        ))
      )
    )
    
    
    
    val origColumns = in0.columns.map(col)
    val out0 = in0
      .groupBy("formulary_name", "customer_name", "run_eff_dt")
      .agg(
        collect_list(
          struct(
            origColumns: _*
          )
        ).alias("inputRows")
      )
      .select(explode(process_udf(col("inputRows"))).alias("output"))
      .select(col("output.*"))
      .withColumn("data_path", op_fl_nn_condition())
    out0
  }

}
