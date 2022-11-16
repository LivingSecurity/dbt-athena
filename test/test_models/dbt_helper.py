import boto3
import json
import os
import time
from subprocess import PIPE, STDOUT, Popen

class DbtHelper:
    def __init__(self):
        self.project_root = os.getenv("DBT_PROJECT_ROOT", "/root/test/test_models/dbt_project")
        self._athena_conn = None
        self.aws_region = os.getenv("DBT_TEST_ATHENA_REGION")
        self.athena_catalog = os.getenv("DBT_TEST_ATHENA_DATABASE")
        self.athena_database = os.getenv("DBT_TEST_ATHENA_SCHEMA")
        self.athena_workgroup = os.getenv("DBT_TEST_ATHENA_WORKGROUP")
    
    @property
    def athena_conn(self):
        if not self._athena_conn:
            self._athena_conn = boto3.client("athena", region_name=self.aws_region)
        return self._athena_conn

    def run_dbt(self, command, model, dbt_vars=None, env=None):
        env_vars = os.environ.copy()

        env = env if env else {}
        if "DBT_PROFILES_DIR" not in env:
            env["DBT_PROFILES_DIR"] = os.path.join(self.project_root, ".dbt")

        env_vars.update(env)

        vars_str = ""
        if dbt_vars:
            vars_str = f"--vars '{json.dumps(dbt_vars)}'"
        dbt_cmd = f"dbt {command} --select {model} {vars_str}"
        print(f"dbt command: {dbt_cmd}")

        sub_process = Popen(
            ["bash", "-c", dbt_cmd],
            stdout=PIPE,
            stderr=STDOUT,
            cwd=self.project_root,
            env=env_vars,
        )

        output = []
        for raw_line in iter(sub_process.stdout.readline, b""):
            line = raw_line.decode("UTF-8").rstrip()
            print(line)
            output.append(line)

        sub_process.wait()
        if sub_process.returncode > 0:
            raise Exception("dbt failed")
        
        return output

    def get_table_contents(self, table_name):
        return self._athena_execute_and_fetch_results(f"SELECT * FROM {self.athena_database}.{table_name}")

    def drop_database(self):
        self._athena_execute_and_fetch_results(f"DROP DATABASE {self.athena_database} CASCADE")

    def _athena_execute_and_fetch_results(self, query, max_checks=20, max_sleep_seconds=30):
        query_execution_context = {
            "Catalog": self.athena_catalog,
            "Database": self.athena_database,
        }

        query_resp = self.athena_conn.start_query_execution(
            QueryString=query,
            QueryExecutionContext=query_execution_context,
            WorkGroup=self.athena_workgroup,
        )
        query_id = query_resp["QueryExecutionId"]

        for i in range(max_checks):
            query_exec = self.athena_conn.get_query_execution(QueryExecutionId=query_id)
            state = query_exec["QueryExecution"]["Status"]["State"]

            if state == "SUCCEEDED":
                return self._athena_fetch_results(query_id)
            elif state in ("FAILED", "CANCELED"):
                raise RuntimeError(f"Athena query ended unexpectedly")
            elif state in ("QUEUED", "RUNNING"):
                to_sleep = min(max_sleep_seconds, 2**i)
                time.sleep(to_sleep)
            else:
                raise ValueError(f"Unhandled query state {state}")
        raise RuntimeError(f"Query didn't complete after max number of checks")

    def _athena_fetch_results(self, query_id):
        results = []
        max_rows = 100
        next_token = None
        done = False

        while not done:
            if next_token:
                res = self.athena_conn.get_query_results(QueryExecutionId=query_id, NextToken=next_token, MaxResults=max_rows)
            else:
                res = self.athena_conn.get_query_results(QueryExecutionId=query_id, MaxResults=max_rows)

            skip_header = True
            for row in res["ResultSet"]["Rows"]:
                if skip_header:
                    skip_header = False
                    continue

                tmp = {}
                for i in range(0, len(row["Data"])):
                    if len(row["Data"][i].keys()) > 0:
                        if "VarCharValue" in row["Data"][i]:
                            tmp[res["ResultSet"]["ResultSetMetadata"]["ColumnInfo"][i]["Name"]] = row["Data"][i][
                                "VarCharValue"
                            ]
                        else:
                            tmp[res["ResultSet"]["ResultSetMetadata"]["ColumnInfo"][i]["Name"]] = None

                results.append(tmp)

            if "NextToken" in res and len(res["ResultSet"]["Rows"]) == max_rows:
                next_token = res["NextToken"]
            else:
                done = True

        return results
