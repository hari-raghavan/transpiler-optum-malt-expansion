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
          lit("cag.C."),
          lit(Config.ENV_NM),
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
            lit("cag.F."),
            lit(Config.ENV_NM),
            lit(".NULL_BASE_LINE."),
            lit(Config.BUSINESS_DATE),
            lit(".dat")
          )
        )
        .when(
          (col("carrier") === lit("*ALL")).and(isnull(col("run_eff_dt"))),
          concat(lit(Config.AI_SERIAL_HOME),
                 lit("/deliver/."),
                 lit("/"),
                 lit(Config.OUTPUT_FILE_PREFIX),
                 lit("cag.C."),
                 lit(Config.ENV_NM),
                 lit(".ALL_ALL_ALL."),
                 lit(Config.BUSINESS_DATE),
                 lit(".dat")
          )
        )
        .when(
          (col("carrier") === lit("*ALL")).and(!isnull(col("run_eff_dt"))),
          concat(lit(Config.AI_SERIAL_HOME),
                 lit("/deliver/."),
                 lit("/"),
                 lit(Config.OUTPUT_FILE_PREFIX),
                 lit("cag.F."),
                 lit(Config.ENV_NM),
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
            lit("cag.C."),
            lit(Config.ENV_NM),
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
            lit("cag.F."),
            lit(Config.ENV_NM),
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
            lit("cag.C."),
            lit(Config.ENV_NM),
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
            lit("cag.F."),
            lit(Config.ENV_NM),
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
            lit("cag.C."),
            lit(Config.ENV_NM),
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
            lit("cag.F."),
            lit(Config.ENV_NM),
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
    
      input.sortBy(r => (r.getAs[String]("run_eff_dt"), r.getAs[Int]("cag_priority"))).foreach { in =>
           val cag_priority = in.getAs[Int]("cag_priority")
           if (cag_priority == 1) {
              baseline_prdcts = in.getAs[Seq[Row]]("prdcts").toArray
              prdcts = baseline_prdcts
            } else if (cag_priority == 2) {
              starall_prdcts = Array.concat(baseline_prdcts.filter { x =>
                                              in.getAs[Seq[Row]]("prdcts").filter(y => y.getAs[String]("ndc11") == x.getAs[String](0)).isEmpty
                                            }.distinct,
                                            in.getAs[Seq[Row]]("prdcts").toArray
              )
              prdcts = starall_prdcts
            } else if (cag_priority == 3) {
              prdcts =
                if (compareTo(starall_prdcts.length, 0) > 0)
                  Array.concat(starall_prdcts.filter(x => in.getAs[Seq[Row]]("prdcts").filter(y => y.getAs[String]("ndc11") == x.getAs[String](0)).isEmpty).distinct,
                               in.getAs[Seq[Row]]("prdcts").toArray
                  )
                else
                  Array.concat(baseline_prdcts.filter(x => in.getAs[Seq[Row]]("prdcts").filter(y => y.getAs[String]("ndc11") == x.getAs[String](0)).isEmpty).distinct,
                               in.getAs[Seq[Row]]("prdcts").toArray
                  )
              c_prdcts = Array.concat(c_prdcts, Array.fill(1)(Row(in.getAs[String]("carrier"), in.getAs[Seq[Row]]("prdcts").toArray)))
            } else if (cag_priority == 4) {
              prdcts = if (compareTo(c_prdcts.length, 0) > 0) {
                if (
                  (try {
                    (c_prdcts
                      .filter(xx => xx.getAs[String](0) == in.getAs[String]("carrier"))
                      .toArray).head
                      .getAs[Seq[Row]](1).toArray
                  } catch {
                    case error: Throwable => null
                  }) != null
                )
                  (c_prdcts
                    .filter(xx => xx.getAs[String](0) == in.getAs[String]("carrier"))
                    .toArray).head
                    .getAs[Seq[Row]](1).toArray
                else
                  Array[Row]()
              } else
                Array[Row]()
              _products = Array.concat(
                prdcts.filter(x => in.getAs[Seq[Row]]("prdcts").filter(y => y.getAs[String]("ndc11") == x.getAs[String](0)).isEmpty).distinct,
                in.getAs[Seq[Row]]("prdcts").toArray
              )
              prdcts = if (compareTo(starall_prdcts.length, 0) > 0) {
                Array.concat(
                  starall_prdcts.filter {
                    x =>
                       _products.filter(y => y.getAs[String](0) == x.getAs[String](0)).isEmpty
                  }.distinct,
                  _products
                )
              } else {
                Array.concat(
                  baseline_prdcts.filter {
                    x =>
                       _products.filter(y => y.getAs[String](0) == x.getAs[String](0)).isEmpty
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
                      .getAs[Seq[Row]](2).toArray
                  } catch {
                    case error: Throwable => null
                  }) != null
                )
                  (ca_prdcts
                    .filter { xx =>
                      (xx.getAs[String](0) == in.getAs[String]("carrier")) && (xx.getAs[String](1) == in.getAs[String]("account"))
                    }
                    .toArray).head
                    .getAs[Seq[Row]](2).toArray
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
                        .getAs[Seq[Row]](1).toArray
                    } catch {
                      case error: Throwable => null
                    }) != null
                  )
                    (c_prdcts
                      .filter(xx => xx.getAs[String](0) == in.getAs[String]("carrier"))
                      .toArray).head
                      .getAs[Seq[Row]](1).toArray
                  else
                    Array[Row]()
              }
              _products = Array.concat(
                prdcts.filter(x => in.getAs[Seq[Row]]("prdcts").filter(y => y.getAs[String]("ndc11") == x.getAs[String](0)).isEmpty).distinct,
                in.getAs[Seq[Row]]("prdcts").toArray
              )
              prdcts = if (compareTo(starall_prdcts.length, 0) > 0) {
                Array.concat(
                  starall_prdcts.filter {
                    x =>
                       _products.filter(y => y.getAs[String](0) == x.getAs[String](0)).isEmpty
                  }.distinct,
                  _products
                )
              } else {
                Array.concat(
                  baseline_prdcts.filter {
                    x =>
                       _products.filter(y => y.getAs[String](0) == x.getAs[String](0)).isEmpty
                  }.distinct,
                  _products
                )
              }
            }
          outputRows.append(Row(
            in.getAs[String]("carrier"),
            in.getAs[String]("account"),
            in.getAs[String]("group"),
            in.getAs[String]("run_eff_dt"),
            in.getAs[Int]("cag_priority"),
            prdcts,
            in.getAs[String]("data_path")
          ))
      }
      outputRows.toArray
    }, 
    ArrayType(
        StructType(
          List(
            StructField("carrier", StringType, false),
            StructField("account", StringType, false),
            StructField("group", StringType, false),
            StructField("run_eff_dt", StringType, false),
            StructField("cag_priority", IntegerType, false),
            StructField(
              "prdcts",
              ArrayType(
                StructType(
                  List(
                    StructField("ndc11",           StringType,         false),
                    StructField("gpi14",           StringType,         false),
                    StructField("status_cd",       StringType,         false),
                    StructField("eff_dt",          StringType,         true),
                    StructField("term_dt",         StringType,         true),
                    StructField("inactive_dt",     StringType,         false),
                    StructField("msc",             StringType,         false),
                    StructField("drug_name",       StringType,         false),
                    StructField("rx_otc",          StringType,         false),
                    StructField("desi",            StringType,         false),
                    StructField("roa_cd",          StringType,         false),
                    StructField("dosage_form_cd",  StringType,         false),
                    StructField("prod_strength",   DecimalType(14, 5), false),
                    StructField("repack_cd",       StringType,         false),
                    StructField("prod_short_desc", StringType,         false),
                    StructField("gpi14_desc",      StringType,         false),
                    StructField("gpi8_desc",       StringType,         false)
                  )
                )
              ),
              false
            ),
            StructField("data_path", StringType, false)
          )
        )
      )
    )
    
    
    
    val origColumns = in0.columns.map(col)
    val out0 = in0
      .groupBy("run_eff_dt")
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
