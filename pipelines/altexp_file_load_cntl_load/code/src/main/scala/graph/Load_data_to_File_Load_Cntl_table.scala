package graph

import io.prophecy.libs._
import config.Context
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Load_data_to_File_Load_Cntl_table {

  def apply(context: Context, in: DataFrame): Unit = {
    val Config = context.config
    import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
    var writer = in.write.format("jdbc")
    writer = writer
      .option("url",      s"${Config.DB_Url}")
      .option("dbtable",  "FA_OWNER.FILE_LOAD_CNTL")
      .option("user",     s"${Config.DB_User}")
      .option("password", s"${Config.DB_Password}")
      .option("driver",   Config.DB_Driver)
    writer.save()
  }

}
