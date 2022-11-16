import pytest

from .dbt_helper import DbtHelper

class TestInsertOverwrite:
    @pytest.fixture(scope="class")
    def dbt_helper(self):
        dbt_helper = DbtHelper()
        yield dbt_helper

        dbt_helper.drop_database()
    
    def test_overwrite_whole_table(self, dbt_helper):
        table_name = "test_overwrite_whole_table"

        dbt_vars = {
            "name": "Sarah Jane",
            "email": "sarah@example.com"
        }

        dbt_helper.run_dbt("run", table_name, dbt_vars=dbt_vars)
        res = dbt_helper.get_table_contents(table_name)

        # one row inserted
        assert len(res) == 1

        # update name and re-run
        dbt_vars["name"] = "Sarah Jones"
        dbt_helper.run_dbt("run", table_name, dbt_vars=dbt_vars)
        res = dbt_helper.get_table_contents(table_name)

        # ensure previous row was deleted and only new row remains
        assert len(res) == 1
        assert res[0]["name"] == dbt_vars["name"]

    def test_insert_overwrite_partition(self, dbt_helper):
        table_name = "test_insert_overwrite_partition"
        dbt_vars = {
            "name": "Sarah Jane",
            "email": "sarah@example.com",
            "partition_key": "pk_alpha",
        }

        dbt_helper.run_dbt("run", table_name, dbt_vars=dbt_vars)

        dbt_vars["partition_key"] = "pk_bravo"
        dbt_helper.run_dbt("run", table_name, dbt_vars=dbt_vars)
        
        res = dbt_helper.get_table_contents(table_name)

        # two rows inserted, one from each partition
        expected_partitions = ["pk_alpha", "pk_bravo"]
        assert len(res) == 2
        assert set([r["partition_key"] for r in res]) == set(expected_partitions)

        # re-run for pk_bravo with new name
        dbt_vars["name"] = "Sarah Jones"
        dbt_helper.run_dbt("run", table_name, dbt_vars=dbt_vars)
        
        res = dbt_helper.get_table_contents(table_name)

        # ensure pk_bravo is updated and pk_alpha is untouched
        expected_names = {"pk_alpha": "Sarah Jane", "pk_bravo": "Sarah Jones"}
        assert len(res) == 2
        assert set([r["partition_key"] for r in res]) == set(expected_partitions)
        assert {r["partition_key"]: r["name"] for r in res} == expected_names

    def test_insert_overwrite_multiple_partitions(self, dbt_helper):
        table_name = "test_insert_overwrite_multiple_partitions"
        dbt_vars = {
            "name": "Sarah Jane",
            "email": "sarah@example.com",
            "pk1": "status=old",
            "pk2": "date=2022-11-01",
        }

        # create 3 partitions: status=old/date=2022-11-01, status=new/date=2022-11-01, status=new/date=2022-11-02
        dbt_helper.run_dbt("run", table_name, dbt_vars=dbt_vars)

        dbt_vars["pk1"] = "status=new"
        dbt_helper.run_dbt("run", table_name, dbt_vars=dbt_vars)

        dbt_vars["pk2"] = "date=2022-11-02"
        dbt_helper.run_dbt("run", table_name, dbt_vars=dbt_vars)

        res = dbt_helper.get_table_contents(table_name)

        assert len(res) == 3
        assert set([f"{r['pk1']}/{r['pk2']}" for r in res]) == set([
            "status=old/date=2022-11-01",
            "status=new/date=2022-11-01",
            "status=new/date=2022-11-02",
        ])

        # update name in partition status=new/date=2022-11-02, ensure others are untouched
        dbt_vars["name"] = "Sarah Jones"
        dbt_helper.run_dbt("run", table_name, dbt_vars=dbt_vars)

        res = dbt_helper.get_table_contents(table_name)
        expected_names = {
            "status=old/date=2022-11-01": "Sarah Jane",
            "status=new/date=2022-11-01": "Sarah Jane",
            "status=new/date=2022-11-02": "Sarah Jones",
        }
        assert len(res) == 3
        assert {f"{r['pk1']}/{r['pk2']}": r["name"] for r in res} == expected_names
