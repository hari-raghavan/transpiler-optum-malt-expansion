package io.prophecy.pipelines.altexp_xml_rules_parser3.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
