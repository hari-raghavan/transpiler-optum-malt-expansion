package io.prophecy.pipelines.altexp_tar_tax_xml_rule_parser_2.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
