# -*- coding: utf-8 -*-
from pathlib import Path
from typing import Any, Dict, Union

import pydeequ
from kedro.framework.context import KedroContext
from pyspark import SparkConf
from pyspark.sql import SparkSession

# ======================================================================
# Maven Coordinates for JARs (and their dependencies) needed to plug
# extra functionality into Spark 2.x (e.g. Kafka SQL and Streaming)
# A one-time internet connection is necessary for Spark to automatically
# download JARs specified by the coordinates (and dependencies).
# ======================================================================
spark_jars_packages = ",".join(
    [
        "com.microsoft.azure:azure-storage:8.6.6",
        "org.apache.hadoop:hadoop-azure:3.3.1",
        "org.firebirdsql.jdbc:jaybird-jdk18:4.0.4.java8",
        "org.postgresql:postgresql:42.2.21",
        "org.mariadb.jdbc:mariadb-java-client:2.7.4",
        "com.amazon.deequ:deequ:1.2.2-spark-3.0",
    ]
)
# ======================================================================


class CustomContext(KedroContext):
    def __init__(
        self,
        package_name: str,
        project_path: Union[Path, str],
        env: str = None,
        extra_params: Dict[str, Any] = None,
    ):
        super().__init__(package_name, project_path, env, extra_params)
        self.init_spark_session()

    def init_spark_session(self) -> None:
        """Initialises a SparkSession using the config
        defined in project's conf folder."""

        # Load the spark configuration in spark.yaml using the config loader
        parameters = self.config_loader.get("spark*", "spark*/**")
        spark_conf = SparkConf()
        spark_conf.setAll(parameters.items())
        spark_conf.set("spark.jars", self.project_path / "jars")
        spark_conf.set(
            "fs.abfs.impl", "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem"
        )

        # Initialise the spark session
        spark_session_conf = (
            SparkSession.builder.appName(self.package_name)
            .master("local[8]")
            .enableHiveSupport()
            .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
            .config("spark.jars.packages", spark_jars_packages)
            .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
            .config(conf=spark_conf)
        )
        _spark_session = spark_session_conf.getOrCreate()
        _spark_session.sparkContext.setLogLevel("WARN")
