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

  lazy val alt_selection_cd_udf = udf(
    { (alt_selection_id: Array[String]) =>
      var result: String = ""
      if (alt_selection_id.nonEmpty) {
        result = if (alt_selection_id(0).length == 11) "NDC" else "GPI"
        alt_selection_id.foreach { s =>
          if (!result.contains("NDC") && s.length == 11) result = result + "NDC"
          else !result.contains("GPI") && s.length == 14
          result = result + "GPI"
        }
      }
      result
    },
    StringType
  )

}
