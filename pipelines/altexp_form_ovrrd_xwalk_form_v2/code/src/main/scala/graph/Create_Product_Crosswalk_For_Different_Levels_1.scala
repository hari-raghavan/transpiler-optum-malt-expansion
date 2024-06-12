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

object Create_Product_Crosswalk_For_Different_Levels_1 {
  def apply(context: Context, in0: DataFrame): DataFrame = {
    val spark = context.spark
    val Config = context.config
    import _root_.io.prophecy.abinitio.ScalaFunctions._
    
    def process_udf(
                    primary_data: Seq[Row], 
                    _formulary_cd: Seq[Seq[String]], 
                    _ndc11: Seq[Seq[String]],
                    _formulary_tier: Seq[Seq[String]],
                    _formulary_status: Seq[Seq[String]],
                    _pa_reqd_ind: Seq[Seq[String]],
                    _specialty_ind: Seq[Seq[String]],
                    _formulary_tier_desc: Seq[Seq[String]],
                    _formulary_status_desc: Seq[Seq[String]],
                    _pa_type_cd: Seq[Seq[String]],
                    _step_therapy_type_cd: Seq[Seq[String]],
                    _step_therapy_group_name: Seq[Seq[String]],
                    _step_therapy_step_number: Seq[Seq[String]],
                    _last_exp_dt: Seq[Seq[String]]
    ) = {
        val outputRows = scala.collection.mutable.ArrayBuffer[Row]()
        var baseline_prdcts = Array[Row]()
        var tarall_prdcts = Array[Row]()
        var c_prdcts = Array[Row]()
        var ca_prdcts = Array[Row]()
        var prdcts = Array[Row]()
        var starall_prdcts = Array[Row]()
        var _products = Array[Row]()
    
        primary_data.zipWithIndex { case (in, idx) =>
            val len = _formulary_cd(idx).length
            val in_prds = (0 until len).map { jdx =>
              Row(_formulary_cd(idx)(jdx),
                  _ndc11(idx)(jdx),
                  _formulary_tier(idx)(jdx),
                  _formulary_status(idx)(jdx),
                  _pa_reqd_ind(idx)(jdx),
                  _specialty_ind(idx)(jdx),
                  _formulary_tier_desc(idx)(jdx),
                  _formulary_status_desc(idx)(jdx),
                  _pa_type_cd(idx)(jdx),
                  _step_therapy_type_cd(idx)(jdx),
                  _step_therapy_group_name(idx)(jdx),
                  _step_therapy_step_number(idx)(jdx),
                  _last_exp_dt(idx)(jdx))
            }.toArray
           val cag_priority = in.getAs[Int](1)
           if (cag_priority == 1) {
              baseline_prdcts = in_prds
              prdcts = baseline_prdcts
            } else if (cag_priority == 2) {
              starall_prdcts = Array.concat(baseline_prdcts.filter { x =>
                                              in_prds.filter(y => (y.getAs[String](1) == x.getAs[String](1) && y.getAs[String](11) == x.getAs[String](11))).isEmpty
                                            }.distinct,
                                            in_prds
              )
              prdcts = starall_prdcts
            } else if (cag_priority == 3) {
              prdcts =
                if (compareTo(starall_prdcts.length, 0) > 0)
                  Array.concat(starall_prdcts.filter(x => in_prds.filter(y => (y.getAs[String](1) == x.getAs[String](1) && y.getAs[String](11) == x.getAs[String](11))).isEmpty).distinct,
                               in_prds.toArray
                  )
                else
                  Array.concat(baseline_prdcts.filter(x => in_prds.filter(y => (y.getAs[String](1) == x.getAs[String](1) && y.getAs[String](11) == x.getAs[String](11))).isEmpty).distinct,
                               in_prds
                  )
              c_prdcts = Array.concat(c_prdcts, Array.fill(1)(Row(in.getAs[String](3), in_prds)))
            } else if (cag_priority == 4) {
              prdcts = if (compareTo(c_prdcts.length, 0) > 0) {
                if (
                  (try {
                    (c_prdcts
                      .filter(xx => xx.getAs[String](0) == in.getAs[String](3))
                      .toArray).head
                      .getAs[Seq[Row]](1).toArray
                  } catch {
                    case error: Throwable => null
                  }) != null
                )
                  (c_prdcts
                    .filter(xx => xx.getAs[String](0) == in.getAs[String](3))
                    .toArray).head
                    .getAs[Seq[Row]](1).toArray
                else
                  Array[Row]()
              } else
                Array[Row]()
              _products = Array.concat(
                prdcts.filter(x => in_prds.filter(y => (y.getAs[String](1) == x.getAs[String](1) && y.getAs[String](11) == x.getAs[String](11))).isEmpty).distinct,
                in_prds.toArray
              )
              prdcts = if (compareTo(starall_prdcts.length, 0) > 0) {
                Array.concat(
                  starall_prdcts.filter {
                    x =>
                       _products.filter(y => (y.getAs[String](1) == x.getAs[String](1) && y.getAs[String](11) == x.getAs[String](11))).isEmpty
                  }.distinct,
                  _products
                )
              } else {
                Array.concat(
                  baseline_prdcts.filter {
                    x =>
                       _products.filter(y => (y.getAs[String](1) == x.getAs[String](1) && y.getAs[String](11) == x.getAs[String](11))).isEmpty
                  }.distinct,
                  _products
                )
              }
              ca_prdcts = Array.concat(
                ca_prdcts,
                Array.fill(1)(
                  Row(
                    in.getAs[String](3),
                    in.getAs[String](4),
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
                        (xx.getAs[String](0) == in.getAs[String](3)) && (xx.getAs[String](1) == in.getAs[String](4))
                      }
                      .toArray).head
                      .getAs[Seq[Row]](2).toArray
                  } catch {
                    case error: Throwable => null
                  }) != null
                )
                  (ca_prdcts
                    .filter { xx =>
                      (xx.getAs[String](0) == in.getAs[String](3)) && (xx.getAs[String](1) == in.getAs[String](4))
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
                        .filter(xx => xx.getAs[String](0) == in.getAs[String](3))
                        .toArray).head
                        .getAs[Seq[Row]](1).toArray
                    } catch {
                      case error: Throwable => null
                    }) != null
                  )
                    (c_prdcts
                      .filter(xx => xx.getAs[String](0) == in.getAs[String](3))
                      .toArray).head
                      .getAs[Seq[Row]](1).toArray
                  else
                    Array[Row]()
              }
              _products = Array.concat(
                prdcts.filter(x => in_prds.filter(y => (y.getAs[String](1) == x.getAs[String](1) && y.getAs[String](11) == x.getAs[String](11))).isEmpty).distinct,
                in_prds
              )
              prdcts = if (compareTo(starall_prdcts.length, 0) > 0) {
                Array.concat(
                  starall_prdcts.filter {
                    x =>
                       _products.filter(y => (y.getAs[String](1) == x.getAs[String](1) && y.getAs[String](11) == x.getAs[String](11))).isEmpty
                  }.distinct,
                  _products
                )
              } else {
                Array.concat(
                  baseline_prdcts.filter {
                    x =>
                       _products.filter(y => (y.getAs[String](1) == x.getAs[String](1) && y.getAs[String](11) == x.getAs[String](11))).isEmpty
                  }.distinct,
                  _products
                )
              }
            }
          prdcts.foreach {  prd =>
            outputRows.append(Row(
              in.getAs[String](2),
              in.getAs[String](3),
              in.getAs[String](4),
              in.getAs[String](5),
              in.getAs[String](6),
              in.getAs[String](7),
              in.getAs[Int](1),
              prd.getAs[String](0),
              prd.getAs[String](1),
              prd.getAs[String](2),
              prd.getAs[String](3),
              prd.getAs[String](4),
              prd.getAs[String](5),
              prd.getAs[String](6),
              prd.getAs[String](7),
              prd.getAs[String](8),
              prd.getAs[String](9),
              prd.getAs[String](10),
              prd.getAs[String](11),
              prd.getAs[String](12),
              prd.getAs[String](13)
            ))
          }
        }
        outputRows.toList
    }
    
    
    val schema = StructType(List(
          StructField("formulary_name", StringType, false),
          StructField("carrier", StringType, false),
          StructField("account", StringType, false),
          StructField("group", StringType, false),
          StructField("customer_name", StringType, false),
          StructField("run_eff_dt", StringType, false),
          StructField("cag_priority", IntegerType, false),
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
        ))
    
    
    val resultRDD = in0.rdd.flatMap(process_udf)
    val out0 = spark.createDataFrame(resultRDD, schema)
    out0
  }

}
