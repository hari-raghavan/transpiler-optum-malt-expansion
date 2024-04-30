package graph.Join_With_DB

import io.prophecy.libs._
import udfs.PipelineInitCode._
import udfs.UDFs._
import graph.Join_With_DB.config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Join_with_DB_jdbc_drop_duplicate {

  def apply(context: Context, in: DataFrame): DataFrame = {
    import org.apache.spark.sql.expressions.Window
    in.dropDuplicates(List("nextval"))
  }

}
