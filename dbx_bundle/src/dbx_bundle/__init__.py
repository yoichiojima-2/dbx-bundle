import os
from pyspark.sql import SparkSession
from databricks.connect import DatabricksSession


os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"


def spark_session() -> SparkSession:
    if hasattr(DatabricksSession.builder, "validateSession"):
        return DatabricksSession.builder.validateSession().getOrCreate()
    else:
        return DatabricksSession.builder.getOrCreate()


spark = spark_session()
