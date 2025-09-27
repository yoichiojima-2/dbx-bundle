import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from databricks.connect import DatabricksSession
from databricks.sdk.runtime import spark as dbx_spark


def connect_remote_spark() -> SparkSession:
    try:
        from dotenv import load_dotenv

        dotenv = Path(__file__).parent.parent.parent / ".env"
        load_dotenv(dotenv)
        print(f"loaded .env: {dotenv}", file=sys.stderr)
    except ImportError:
        pass

    builder = DatabricksSession.builder

    if os.getenv("DATABRICKS_SERVERLESS", "").lower() == "true":
        print("connecting serverless clusters", file=sys.stderr)
        builder = builder.serverless(True)
    elif cluster_id := os.getenv("DATABRICKS_CLUSTER_ID"):
        print(f"connecting {cluster_id}", file=sys.stderr)
        builder = builder.clusterId(cluster_id)
    else:
        raise ValueError(
            "neither DATABRICKS_SERVERLESS / DATABRICKS_CLUSTER_ID are not set properly"
        )

    return builder.getOrCreate()


def spark_session() -> SparkSession:
    if dbx_spark is not None:
        return dbx_spark
    else:
        return connect_remote_spark()


spark = spark_session()
