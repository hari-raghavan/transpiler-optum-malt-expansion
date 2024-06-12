package udfs

import _root_.io.prophecy.abinitio.ScalaFunctions._
import _root_.io.prophecy.libs._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object UDFs extends Serializable {

  def registerUDFs(spark: SparkSession) =
    registerAllUDFs(spark)

}

object PipelineInitCode extends Serializable {

  def op_fl_nn_condition(
    AI_SERIAL_HOME:     Column,
    OUTPUT_FILE_PREFIX: Column,
    ENV_NM:             Column,
    BUSINESS_DATE:      Column
  ) =
    when(
      isnull(col("carrier"))
        .and(isnull(col("account")))
        .and(isnull(col("group")))
        .and(isnull(col("run_eff_dt"))),
      concat(
        AI_SERIAL_HOME,
        lit("/deliver/."),
        lit("/"),
        OUTPUT_FILE_PREFIX,
        lit("form.C."),
        ENV_NM,
        lit("."),
        col("customer_name"),
        lit("."),
        regexp_replace(col("formulary_name"), lit(" +"), lit("-")),
        lit(".NULL_BASE_LINE."),
        BUSINESS_DATE,
        lit(".dat")
      )
    ).when(
        isnull(col("carrier"))
          .and(isnull(col("account")))
          .and(isnull(col("group")))
          .and(!isnull(col("run_eff_dt"))),
        concat(
          AI_SERIAL_HOME,
          lit("/deliver/."),
          lit("/"),
          OUTPUT_FILE_PREFIX,
          lit("form.F."),
          ENV_NM,
          lit("."),
          col("customer_name"),
          lit("."),
          regexp_replace(col("formulary_name"), lit(" +"), lit("-")),
          lit(".NULL_BASE_LINE."),
          BUSINESS_DATE,
          lit(".dat")
        )
      )
      .when(
        (col("carrier") === lit("*ALL")).and(isnull(col("run_eff_dt"))),
        concat(
          AI_SERIAL_HOME,
          lit("/deliver/."),
          lit("/"),
          OUTPUT_FILE_PREFIX,
          lit("form.C."),
          ENV_NM,
          lit("."),
          col("customer_name"),
          lit("."),
          regexp_replace(col("formulary_name"), lit(" +"), lit("-")),
          lit(".ALL_ALL_ALL."),
          BUSINESS_DATE,
          lit(".dat")
        )
      )
      .when(
        (col("carrier") === lit("*ALL")).and(!isnull(col("run_eff_dt"))),
        concat(
          AI_SERIAL_HOME,
          lit("/deliver/."),
          lit("/"),
          OUTPUT_FILE_PREFIX,
          lit("form.F."),
          ENV_NM,
          lit("."),
          col("customer_name"),
          lit("."),
          regexp_replace(col("formulary_name"), lit(" +"), lit("-")),
          lit(".ALL_ALL_ALL."),
          BUSINESS_DATE,
          lit(".dat")
        )
      )
      .when(
        (!isnull(col("carrier")))
          .and(col("account") === lit("*ALL"))
          .and(col("group") === lit("*ALL"))
          .and(isnull(col("run_eff_dt"))),
        concat(
          AI_SERIAL_HOME,
          lit("/deliver/."),
          lit("/"),
          OUTPUT_FILE_PREFIX,
          lit("form.C."),
          ENV_NM,
          lit("."),
          col("customer_name"),
          lit("."),
          regexp_replace(col("formulary_name"), lit(" +"), lit("-")),
          lit("."),
          col("carrier"),
          lit("_ALL_ALL."),
          BUSINESS_DATE,
          lit(".dat")
        )
      )
      .when(
        (!isnull(col("carrier")))
          .and(col("account") === lit("*ALL"))
          .and(col("group") === lit("*ALL"))
          .and(!isnull(col("run_eff_dt"))),
        concat(
          AI_SERIAL_HOME,
          lit("/deliver/."),
          lit("/"),
          OUTPUT_FILE_PREFIX,
          lit("form.F."),
          ENV_NM,
          lit("."),
          col("customer_name"),
          lit("."),
          regexp_replace(col("formulary_name"), lit(" +"), lit("-")),
          lit("."),
          col("carrier"),
          lit("_ALL_ALL."),
          BUSINESS_DATE,
          lit(".dat")
        )
      )
      .when(
        (!isnull(col("carrier")))
          .and(!isnull(col("account")))
          .and(col("group") === lit("*ALL"))
          .and(isnull(col("run_eff_dt"))),
        concat(
          AI_SERIAL_HOME,
          lit("/deliver/."),
          lit("/"),
          OUTPUT_FILE_PREFIX,
          lit("form.C."),
          ENV_NM,
          lit("."),
          col("customer_name"),
          lit("."),
          regexp_replace(col("formulary_name"), lit(" +"), lit("-")),
          lit("."),
          col("carrier"),
          lit("_"),
          col("account"),
          lit("_ALL."),
          BUSINESS_DATE,
          lit(".dat")
        )
      )
      .when(
        (!isnull(col("carrier")))
          .and(!isnull(col("account")))
          .and(col("group") === lit("*ALL"))
          .and(!isnull(col("run_eff_dt"))),
        concat(
          AI_SERIAL_HOME,
          lit("/deliver/."),
          lit("/"),
          OUTPUT_FILE_PREFIX,
          lit("form.F."),
          ENV_NM,
          lit("."),
          col("customer_name"),
          lit("."),
          regexp_replace(col("formulary_name"), lit(" +"), lit("-")),
          lit("."),
          col("carrier"),
          lit("_"),
          col("account"),
          lit("_ALL."),
          BUSINESS_DATE,
          lit(".dat")
        )
      )
      .when(
        isnull(col("run_eff_dt")),
        concat(
          AI_SERIAL_HOME,
          lit("/deliver/."),
          lit("/"),
          OUTPUT_FILE_PREFIX,
          lit("form.C."),
          ENV_NM,
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
          BUSINESS_DATE,
          lit(".dat")
        )
      )
      .otherwise(
        concat(
          AI_SERIAL_HOME,
          lit("/deliver/."),
          lit("/"),
          OUTPUT_FILE_PREFIX,
          lit("form.F."),
          ENV_NM,
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
          BUSINESS_DATE,
          lit(".dat")
        )
      )

}
