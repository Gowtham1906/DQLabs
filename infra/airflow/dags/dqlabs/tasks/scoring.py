"""
    Migration Notes From V2 to V3:
    Migrations Completed 
    Pending:true
    Validate Scoring Logic
"""

# Import Helpers and Utils
from dqlabs.utils.scoring import (
    get_attribute_scores,
    get_rules_statistics,
    get_asset_scores,
)
from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.db_helper import execute_query, fetchall

from dqlabs.app_helper.lineage_helper import update_report_pipeline_propagations


def update_scores(config: dict) -> None:
    """
    Updates the dqscore of the last run for the given asset
    into postgres
    """
    asset_id = config.get("asset_id")
    attribute_scores = get_attribute_scores(config)
    asset_scores = get_asset_scores(config)
    rules_statistics = get_rules_statistics(config)

    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        # Updated attribute level score
        total_measures = 0
        total_passed_measures = 0
        total_failed_measures = 0
        for attribute_id, score in attribute_scores.items():
            if score:
                score = 100 if score > 100 else score
                score = 0 if score < 0 else score
                # score = round(score, 2)
            rule_stat = rules_statistics.get(attribute_id, {})
            rule_stat = rule_stat if rule_stat else {}

            fields_to_update = []
            if rule_stat:
                attribute_rules_count = rule_stat.get("total_rules", 0)
                passed_rules = rule_stat.get("passed_rules", 0)
                failed_rules = rule_stat.get("failed_rules", 0)

                fields_to_update.append(f"passed_rules={passed_rules}")
                fields_to_update.append(f"failed_rules={failed_rules}")
                fields_to_update.append(f"total_rules={attribute_rules_count}")

                total_passed_measures = total_passed_measures + passed_rules
                total_failed_measures = total_failed_measures + failed_rules
                total_measures = total_measures + attribute_rules_count

            fields = ", ".join(fields_to_update)
            fields = f", {fields}" if fields else ""

            if attribute_id:
                score = "null" if score is None else score
                query_string = f"""
                    update core.attribute set score={score} {fields}
                    where asset_id='{asset_id}' and id='{attribute_id}'
                """
                cursor = execute_query(connection, cursor, query_string)

        # Update asset level score for the current run
        score = None
        if asset_scores and asset_id in asset_scores:
            score = asset_scores.get(asset_id, 0)
        score = "null" if score is None else score
        query_string = f"""
            update core.asset set score={score} where id='{asset_id}'
        """
        cursor = execute_query(connection, cursor, query_string)

        query_string = f"""
            update core.data
            set passed_rules={total_passed_measures},
            failed_rules={total_failed_measures}, total_rules={total_measures}
            where id='{asset_id}'
        """
        cursor = execute_query(connection, cursor, query_string)

        # Update DQscore for Reports and Pipelines
        query_string = f"""
                select 
                    asset.*,
                    connection.credentials
                from core.associated_asset
                join core.asset on asset.id = associated_asset.source_asset_id
                join core.connection on connection.id = asset.connection_id            
                where associated_asset_id = '{asset_id}'
        """
        cursor = execute_query(connection, cursor, query_string)
        mapped_data = fetchall(cursor)
        for m_data in mapped_data:
            if m_data:
                update_report_pipeline_propagations(
                    config, m_data)
