"""How the tools reach the offline store: from the environment, and only from it.

No credential is defaulted in code. A fallback here would be a credential that
ships with the tool, reaches production the first time someone forgets to set
the real one, and works well enough there that nobody notices.

Where the environment gets them depends on who is running:

  local stack   showcase/infra/local.env, which the Makefile and docker compose
                both read. It holds the throwaway keys of the SeaweedFS that
                infra/seaweed/s3.json configures, and nothing else.
  Airflow       the `fs_object_store` Connection, rendered into each task's
                environment at run time (see the DAG). A deployment backs it
                with its secrets manager.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

import duckdb

REQUIRED = ("S3_ENDPOINT", "S3_ACCESS_KEY", "S3_SECRET_KEY")


@dataclass(frozen=True)
class ObjectStore:
    endpoint: str  # host:port, no scheme
    access_key: str
    secret_key: str

    @classmethod
    def from_env(cls) -> ObjectStore:
        missing = [k for k in REQUIRED if not os.environ.get(k)]
        if missing:
            raise SystemExit(
                f"object store not configured: {', '.join(missing)} unset. Run through the "
                "showcase Makefile, which loads infra/local.env for the local stack, or "
                "export them from your secret store."
            )
        return cls(
            endpoint=os.environ["S3_ENDPOINT"],
            access_key=os.environ["S3_ACCESS_KEY"],
            secret_key=os.environ["S3_SECRET_KEY"],
        )

    def configure(self, con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyConnection:
        """Point a DuckDB connection's httpfs at this store."""
        con.execute("install httpfs; load httpfs;")
        con.execute(f"set s3_endpoint='{self.endpoint}'")
        con.execute(f"set s3_access_key_id='{self.access_key}'")
        con.execute(f"set s3_secret_access_key='{self.secret_key}'")
        con.execute("set s3_use_ssl=false; set s3_url_style='path'; set s3_region='us-east-1'")
        return con

    def boto3_client(self):
        import boto3

        return boto3.client(
            "s3",
            endpoint_url=f"http://{self.endpoint}",
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name="us-east-1",
        )
