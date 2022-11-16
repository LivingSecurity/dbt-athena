import pytest

from .dbt_helper import DbtHelper


class TestIcebergModels:
    @pytest.fixture(scope="class")
    def dbt_helper(self):
        dbt_helper = DbtHelper()
        yield dbt_helper

        dbt_helper.drop_database()

    def test_merge(self, dbt_helper):
        table_name = "test_merge"
        dbt_vars = {
            "name": "Sarah Jane",
            "email": "sarah@example.com",
            "include_dave": "false"
        }

        dbt_helper.run_dbt("run", table_name, dbt_vars=dbt_vars)
        res = dbt_helper.get_table_contents(table_name)

        assert len(res) == 1
        assert res[0]["name"] == dbt_vars["name"]
        assert res[0]["email"] == dbt_vars["email"]

        # sarah is updated and dave is inserted
        dbt_vars["name"] = "Sarah Jones"
        dbt_vars["include_dave"] = "true"

        dbt_helper.run_dbt("run", table_name, dbt_vars=dbt_vars)
        res = dbt_helper.get_table_contents(table_name)

        expected_names = ["Dave Patterson", "Sarah Jones"]
        expected_emails = ["david.patterson@example.com", "sarah@example.com"]
        assert len(res) == 2
        assert set([r["name"] for r in res]) == set(expected_names)
        assert set([r["email"] for r in res]) == set(expected_emails)

        # ensure we don't lose dave after re-running without him in stage
        dbt_vars["include_dave"] = "false"
        dbt_helper.run_dbt("run", table_name, dbt_vars=dbt_vars)
        res = dbt_helper.get_table_contents(table_name)

        assert len(res) == 2
        assert set([r["name"] for r in res]) == set(expected_names)
        assert set([r["email"] for r in res]) == set(expected_emails)

    def test_merge_no_partition_values(self, dbt_helper):
        table_name = "test_merge_no_partition_values"
        dbt_helper.run_dbt("run", table_name)

        # empty table should have been created
        res = dbt_helper.get_table_contents(table_name)
        assert len(res) == 0

    def test_merge_partitioned(self, dbt_helper):
        table_name = "test_merge_partitioned"

        dbt_vars = {"partition_key": "pk_alpha"}
        dbt_helper.run_dbt("run", table_name, dbt_vars=dbt_vars)

        # two rows created in pk_alpha partition
        res = dbt_helper.get_table_contents(table_name)
        assert len(res) == 2

        dbt_vars["partition_key"] = "pk_bravo"
        dbt_helper.run_dbt("run", table_name, dbt_vars=dbt_vars)

        # two new rows created in pk_bravo partition
        res = dbt_helper.get_table_contents(table_name)
        assert len(res) == 4

        expected_row_counts = {"pk_alpha": 2, "pk_bravo": 2}
        row_counts = {}
        for row in res:
            if row["partition_key"] not in row_counts:
                row_counts[row["partition_key"]] = 0
            row_counts[row["partition_key"]] += 1

        assert row_counts == expected_row_counts
