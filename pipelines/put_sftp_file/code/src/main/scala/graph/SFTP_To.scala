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

object SFTP_To {
  def apply(context: Context, in: DataFrame): Unit = {
    val spark = context.spark
    val Config = context.config
    
      /*
       * Add dependency as given below to use this Component.
       * <dependency>
       *	<groupId>com.springml</groupId>
       *	<artifactId>spark-sftp_2.11</artifactId>
       *	<version>1.1.3</version>
       * </dependency>
       */
    
      in.write
        .format("com.springml.spark.sftp")
        .option("host",     s"${Config.SFTP_HOST}")
        .option("username", s"${Config.SFTP_USER}")
        .option("password", s"${Config.SFTP_PASSWORD}")
        .option("fileType", "txt")
        .save(s"NA")
  }

}
