package io.prophecy.pipelines.altexp_tac_rules_parser_2.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
