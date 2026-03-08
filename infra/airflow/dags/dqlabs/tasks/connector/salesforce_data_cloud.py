import json
from uuid import uuid4
from datetime import datetime, timedelta

from dqlabs.app_helper.db_helper import execute_query, fetchall, fetchone, split_queries
from dqlabs.app_helper.connection_helper import decrypt_connection_config
from dqlabs.app_helper.dag_helper import (
    get_postgres_connection,
)
from dqlabs.app_helper.lineage_helper import get_asset_metric_count, save_lineage, save_asset_lineage_mapping, save_lineage_entity, update_asset_metric_count, update_pipeline_propagations, update_reports_propagations, handle_alerts_issues_propagation
from dqlabs.app_helper.log_helper import log_error, log_info
from dqlabs.utils.tasks import get_task_config, check_task_status
from dqlabs.utils.request_queue import (
    update_queue_detail_status,
    update_queue_status,
    create_queue_detail,
)
from dqlabs.app_helper.dag_helper import (
    execute_native_query,
    get_postgres_connection,
    get_native_connection
)
from dqlabs.utils.extract_workflow import (
    get_queries,
    update_asset_run_id
)
from dqlabs.enums.schedule_types import ScheduleStatus
from dqlabs.app_helper import agent_helper
from dqlabs.app_helper.pipeline_helper import get_pipeline_last_run_id, get_pipeline_id, update_pipeline_last_runs
from dqlabs.app_helper.dq_helper import get_pipeline_status
from dqlabs.enums.connection_types import ConnectionType
from dqlabs.utils.pipeline_measure import execute_pipeline_measure, create_pipeline_task_measures, get_pipeline_tasks
from dqlabs.app_helper.lineage_helper import save_reports_lineage, map_asset_with_lineage, update_reports_propagations, update_report_last_runs, save_asset_lineage_mapping


TASK_CONFIG = None
LATEST_RUN = False
ALL_RUNS = []

def extract_salesforce_data_cloud(config, **kwargs):
    try:
        is_completed = check_task_status(config, kwargs)
        if is_completed:
            return

        # Update the asset state
        run_id = config.get("queue_id")
        asset = config.get("asset")
        default_queries = get_queries(config)
        asset = asset if asset else {}
        asset_properties = asset.get("properties", {})
        asset_properties = asset_properties if asset_properties else {}
        asset_id = asset.get("id")
        global TASK_CONFIG
        global LATEST_RUN
        global ALL_RUNS
        task_config = get_task_config(config, kwargs)
        update_queue_detail_status(
            config, ScheduleStatus.Running.value, task_config=task_config
        )
        update_queue_status(config, ScheduleStatus.Running.value, True)
        update_asset_run_id(run_id, config)

        connection = config.get("connection", {})
        credentials = connection.get("credentials")
        connection_type = connection.get("type", "")
        credentials = decrypt_connection_config(credentials, connection_type)
        source_connection = get_native_connection(config)
        metadata_pull_config = credentials.get("metadata", {})
        runs_pull = metadata_pull_config.get("runs", False)
        tasks_pull = metadata_pull_config.get("tasks", False)
        tranformation_pull = metadata_pull_config.get("transform", False)
        pipeline_id = __get_pipeline_id(config)
        TASK_CONFIG = config

        data = {}
        pipeline_info = {}
        pipeline_runs = []
        all_runs = []
        latest_run = False

        pipeline_info = __get_pipeline_detail(config, asset_properties, credentials)
        if tasks_pull:
            if pipeline_info:
                # save tasks in pipeline_task table
                new_tasks = __save_pipeline_tasks(
                    config,
                    pipeline_info,
                    pipeline_id,
                    asset_id,
                    asset_properties,
                    "task",
                    all_runs
                )
        
            # Update Propagations Alerts and Issues Creation and Notifications
        if runs_pull:
            pipeline_runs = __get_pipeline_runs(config, asset, credentials)
            if pipeline_runs:
                latest_run, all_runs = __save_pipeline_runs(
                    config,
                    pipeline_info,
                    asset_id,
                    pipeline_id,
                    "pipeline",
                    pipeline_runs
                )
        update_pipeline_propagations(config, asset, connection_type)
        __update_last_run_stats(config, "pipeline",connection_type)
        # get pipeline details
        if latest_run and all_runs:
            # Get the latest run from database to ensure we have the most recent run_id
            db_connection = get_postgres_connection(config)
            with db_connection.cursor() as cursor:
                query_string = f"""
                    select source_id
                    from core.pipeline_runs
                    where asset_id = '{asset_id}'
                    order by run_end_at desc
                    limit 1
                """
                cursor = execute_query(db_connection, cursor, query_string)
                latest_run_result = fetchone(cursor)
                latest_run_id = latest_run_result.get("source_id") if latest_run_result else ""
                if latest_run_id:
                    handle_alerts_issues_propagation(config, latest_run_id)

        # prepare lineage
        lineage =__prepare_lineage(config, pipeline_info, asset,all_runs)
        save_lineage(config, "pipeline", lineage, asset.get("id"))
        update_reports_propagations(config, config.get("asset", {}))
        # Auto-map SDC lineage entities to existing assets (modeled after Coalesce)
        try:
            __map_asset_with_lineage(config, lineage)
        except Exception as e:
            log_error("Salesforce Data Cloud - map asset with lineage failed", e)
        # Save transformations similar to ADF
        try:
            if tranformation_pull:
                pipeline_id = __get_pipeline_id(config)
                __save_sdc_transformations(config, pipeline_info, pipeline_id, asset, lineage,all_runs)
        except Exception as e:
                log_error("Salesforce Data Cloud - save transformations failed", e)

           # save Propagation Values
        update_pipeline_propagations(config, asset, connection_type)


        # # Update Job Run Stats
        __update_pipeline_stats(config)
        propagate_alerts = credentials.get("propagate_alerts", "table")
        # Execute pipeline and task measures (similar to DBT) without altering main flow
        metrics_count = None
        if propagate_alerts == "table":
            metrics_count = get_asset_metric_count(config, asset_id)
        try:
            if latest_run:
                __extract_sdc_pipeline_measure(config)
        except Exception as e:
            log_error("Salesforce Data Cloud - extract pipeline measures failed", e)
        if propagate_alerts == "table" or propagate_alerts == "pipeline":
            update_asset_metric_count(config, asset_id, metrics_count, latest_run, propagate_alerts=propagate_alerts)

        # # Get Description and Prepare Search Key
        description = __prepare_description(asset)
        description = description.replace("'", "''")

        # Get Description and Prepare Search Key
        search_keys = __prepare_search_key(asset_properties)
        search_keys = search_keys.replace("'", "''")
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                update core.asset set description='{description}', search_keys='{search_keys}'
                where id = '{asset_id}' and is_active=true and is_delete = false
            """
            cursor = execute_query(connection, cursor, query_string)

        # # update request queue status
        is_triggered = config.get("is_triggered")
        if not is_triggered:
            create_queue_detail(config)
        update_queue_detail_status(config, ScheduleStatus.Completed.value)
    except Exception as e:
        log_error("Salesforce Data Cloud Pull / Push Failed", e)
        update_queue_detail_status(config, ScheduleStatus.Failed.value, str(e))
    finally:
        update_queue_status(config, ScheduleStatus.Completed.value)

def __update_last_run_stats(config: dict, type: str, connection_type: str = "") -> str:
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select source_id as run_id,status, run_start_at, run_end_at, duration from core.pipeline_runs
                where asset_id = '{asset_id}' order by run_start_at desc
                limit 1
            """
            cursor = execute_query(connection, cursor, query_string)
            last_run = fetchone(cursor)
            if last_run:
                run_id = last_run.get('run_id') 
                status = last_run.get('status')
                last_run_at = last_run.get('run_start_at') if type == "pipeline" else last_run.get('run_end_at')
                pipeline_query_string = ""
                pipeline_query_string = f"""
                    update core.pipeline set run_id='{run_id}', status='{status}', last_run_at='{last_run_at}'
                    where asset_id = '{asset_id}'
                """
                execute_query(connection, cursor, pipeline_query_string)
                last_run_start_at = last_run.get('run_start_at')
                last_run_end_at = last_run.get('run_end_at')
                duration = last_run.get('duration')
                update_task_query = f""" update core.pipeline_tasks set run_id='{run_id}', run_start_at='{last_run_start_at}', run_end_at='{last_run_end_at}', duration='{duration}' where asset_id = '{asset_id}'"""
                execute_query(connection, cursor, update_task_query)
    except Exception as e:
        log_error(f"Salesforce Data Cloud Pipeline Connector - Update Run Stats to Job Failed ", e)
        raise e

def __get_pipeline_runs(config, asset, credentials):
    try:
        no_of_days = credentials.get("no_of_runs", 30)
        # Calculate timestamp for n days ago
        days_ago_date = datetime.now() - timedelta(days=int(no_of_days))
        status = (credentials.get("status") or "all")
        api_response = __get_response(
            config,
            "post",
            { 
                "after_date": str(days_ago_date),
                "status": status,
                "name": asset.get("name").strip(),
                "pipeline_id": asset.get("properties").get("pipeline_id")
            },
            method_name="get_pipeline_runs"
        )
         # Navigate through the nested structure
        results = (api_response)
        
        if results and isinstance(results, list):
            # - 0: Error
            # - 1: Completed
            # - 2: Active (Running)
            # - 3: Waiting
            # - 4: Cancelled
            # - 5: Validating
            # - 6: Ready
            # - 7: Initializing

            def _normalize_status(value):
                if value is None:
                    return None
                v = str(value).strip().lower()
                if v in ["1", "completed", "complete", "success", "succeeded"]:
                    return "success"
                if v in ["0", "error", "failed", "failure", "cancelled", "canceled"]:
                    return "failed"
                if v in ["2", "running", "active"]:
                    return "running"
                return v

            selected_status = _normalize_status(status)
            is_filter_applied = False if selected_status == "all" else selected_status in ["success", "failed", "running"]
            
            def _parse_dt(value):
                try:
                    if isinstance(value, str):
                        currentDate = datetime.fromisoformat(value.replace('Z', '+00:00')).replace(tzinfo=None)
                        return datetime.fromisoformat(value.replace('Z', '+00:00')).replace(tzinfo=None)
                    if isinstance(value, (int, float)):
                        ts = value / 1000 if value > 1e12 else value
                        return datetime.fromtimestamp(ts)
                except Exception:
                    return None

            cutoff_date = days_ago_date
            filtered_results = []
            for item in results:
                end_time_ok = item.get("endTime") and _parse_dt(item.get("endTime")) and _parse_dt(item.get("endTime")) >= cutoff_date
                if not end_time_ok:
                    continue
                item_status = _normalize_status(item.get("status"))
                if not is_filter_applied:
                    filtered_results.append(item)
                    continue
                if selected_status == "success" and item_status == "success":
                    filtered_results.append(item)
                elif selected_status == "failed" and item_status == "failed":
                    filtered_results.append(item)
                elif selected_status == "running" and item_status == "running":
                    filtered_results.append(item)
            return filtered_results
        else:
            return []
        pass
    except Exception as e:
        log_error("Salesforce Data Cloud Runs Failed", e)

def __prepare_lineage(config, pipeline_info, asset,all_runs):
    """
    Helper function to prepare Lineage
    """
    try:
        # If Salesforce Data Cloud definition exists, build lineage based on nodes (ADF-style)
        nodes = (pipeline_info.get("definition", {}) or {}).get("nodes", {})
        if isinstance(nodes, dict) and nodes:
            lineage = __prepare_sdc_lineage(config, pipeline_info)
            # Create table entities for load/output nodes
            try:
                tables_only = [t for t in lineage.get("tables", []) ]
                if tables_only:
                    save_lineage_entity(config, tables_only, asset.get("id"), True)
            except Exception as e:
                log_error("Salesforce Data Cloud - save_lineage_entity failed", e)
            return lineage

    
    except Exception as e:
        log_error(f"Salesforce Data Cloud Connector - Prepare Lineage Table Failed ", e)
        raise e



def __prepare_sdc_lineage(config, pipeline_info: dict) -> dict:
    """
    Prepare lineage for Salesforce Data Cloud (ADF-style):
    - Tables: load and outputD360 nodes only
    - Transformations: all other non-extract nodes (aggregate, filter, join, etc.)
    - Ignore: extract/extractGrains nodes
    - Relations: connect sequence along flows while bypassing extract nodes
    """
    lineage = {"tables": [], "relations": []}
    try:
        definition = pipeline_info.get("definition", {}) or {}
        nodes = definition.get("nodes", {}) or {}
        # Determine whether to use transformation source_id or entity_name in relations
        transformation_pull = False
        try:
            conn = (config.get("connection") or {})
            conn_type = conn.get("type", "")
            raw_creds = conn.get("credentials")
            creds = decrypt_connection_config(raw_creds, conn_type) if raw_creds is not None else {}
            meta = (creds.get("metadata") or {}) if isinstance(creds, dict) else {}
            transformation_pull = bool(meta.get("transform", False))
        except Exception:
            transformation_pull = False
        connection_data = config.get("connection", {})
        connection_id = connection_data.get("id")

        def is_load(node):
            return ((node or {}).get("action", "").strip().lower() == "load")

        def is_output(node):
            a = (node or {}).get("action", "").strip().lower()
            return a in ["outputd360", "output", "outputdataset"]

        def is_extract(node):
            a = (node or {}).get("action", "").strip().lower()
            return a in ["extractgrains", "extract"]

        def classify(node_id, node):
            if is_extract(node):
                return "ignore"
            if is_load(node) or is_output(node):
                return "table"
            return "transformation"

        # Build adjacency from sources -> node
        adjacency = {}
        for node_id, node in nodes.items():
            for src in (node or {}).get("sources", []) or []:
                adjacency.setdefault(src, []).append(node_id)

        # Compute grouped flow task id per node (load..output path) to attach task_id into transformation source_id
        load_node_ids = [nid for nid, nn in nodes.items() if is_load(nn)]
        output_node_ids = set([nid for nid, nn in nodes.items() if is_output(nn)])

        def _dfs_paths(start: str, target_set: set) -> list:
            all_paths = []
            visited = set()

            def _dfs(curr: str, path: list):
                if curr in visited:
                    return
                path.append(curr)
                visited.add(curr)
                if curr in target_set:
                    all_paths.append(list(path))
                for nxt in adjacency.get(curr, []) or []:
                    _dfs(nxt, path)
                visited.remove(curr)
                path.pop()

            _dfs(start, [])
            return all_paths

        node_to_flow_task_id = {}
        for ld in load_node_ids:
            paths = _dfs_paths(ld, output_node_ids)
            for p in paths:
                if not p:
                    continue
                start = p[0]
                end = p[-1]
                flow_task_id = f"task.flow.{str(connection_id)}.{start.lower()}__to__{end.lower()}"
                for nid in p:
                    if not is_extract(nodes.get(nid)):
                        node_to_flow_task_id.setdefault(nid, flow_task_id)

        tables = []
        transformations = []
        id_map = {}  # map node id -> lineage entity id

        # Create entities
        for node_id, node in nodes.items():
            node_type = classify(node_id, node)
            if node_type == "ignore":
                continue

            action = (node or {}).get("action")
            parms = (node or {}).get("parameters", {}) or {}
            sources = (node or {}).get("sources", []) or []

            # Determine per-node flow task_id (if part of any path) for transformation source ids
            flow_task_id = node_to_flow_task_id.get(node_id)

            entity = {
                "id": str(uuid4()),
                "name": node_id,
                "source_type": "Table" if node_type == "table" else "Transformation",
                "type": "table" if node_type == "table" else "transform",
                "connection_type": "salesforce_data_cloud",
                "level": 2 if node_type == "table" else 3,
                "action": action,
                "parameters": parms,
                "sources": sources,
                "entity_name": node_id
            }

            # For transformation nodes, attach a unique source_id including connection and task id
            if node_type != "table":
                # Build a deterministic transformation source id
                trans_source_id = (
                    f"transformation.{str(connection_id)}.{flow_task_id}.{node_id}.transformation"
                    if flow_task_id
                    else f"transformation.{str(connection_id)}.{node_id}.transformation"
                )
                entity.update({
                    "source_id": trans_source_id,
                    "task_id": flow_task_id,
                })

            # Map node id to identifier used in relations:
            # - For tables: prefer friendly entity name (normalized dataset/object name)
            # - For transformations: use source_id when transformation_pull is enabled; else entity_name
            if node_type == "table":
                id_map[node_id] = entity["entity_name"]
            else:
                id_map[node_id] = entity.get("source_id") if transformation_pull else entity["entity_name"]

            if node_type == "table":
                # For load: capture dataset; for output: capture output object
                if is_load(node):
                    ds = (parms.get("dataset") or {})
                    # Prefer dataset name for entity/name, normalizing double underscores
                    dataset_name_raw = ds.get("name", "")
                    normalized_dataset_name = dataset_name_raw if dataset_name_raw else node_id
                    # Fetch attributes to enrich field types
                    attrs = __get_sdc_attributes(
                        config,
                        table_name=ds.get("name", ""),
                        asset_type=ds.get("type", ""),
                    )
                    attr_map = {}
                    try:
                        attr_map = {str(a.get("name", "")): a for a in (attrs or [])}
                    except Exception:
                        attr_map = {}
                    fields_list = []
                    for f in (parms.get("fields") or []):
                        a = attr_map.get(str(f)) if attr_map else None
                        fields_list.append({
                            "name": f,
                            "data_type": (a or {}).get("data_type", ""),
                            "id": str(uuid4()),
                        })
                    # If API returned attributes but the parameter list is empty, fallback to attributes
                    if not fields_list and attrs:
                        for a in attrs:
                            fields_list.append({
                                "name": str(a.get("name", "")),
                                "data_type": str(a.get("data_type", "")),
                                "id": str(uuid4()),
                            })
                    entity.update({
                        "name": normalized_dataset_name,
                        "entity_name": normalized_dataset_name,
                        "dataset_name": ds.get("name", ""),
                        "dataset_type": ds.get("data_type", ""),
                        "fields": fields_list,
                    })
                elif is_output(node):
                    obj_name = parms.get("name", "")
                    normalized_obj_name = obj_name if obj_name else node_id
                    output_obj = None
                    for o in (definition.get("outputDataObjects") or []):
                        if o.get("name") == obj_name:
                            output_obj = o
                            break
                    fields = []
                    if output_obj:
                        fields = [
                            {
                                "name": (fld or {}).get("name", ""),
                                "data_type": (fld or {}).get("type", ""),
                                "isPrimaryKey": (fld or {}).get("isPrimaryKey", False),
                                "id": str(uuid4()),
                            }
                            for fld in (output_obj.get("fields") or [])
                        ]
                    entity.update({
                        "name": normalized_obj_name,
                        "entity_name": normalized_obj_name,
                        "object_name": obj_name,
                        "fields": fields,
                    })
                tables.append(entity)
            else:
                transformations.append(entity)

        # Relations: connect neighbors skipping extracts
        relations = []
        added_pairs = set()

        def non_extract_upstream(start_id: str) -> set:
            """Walk upstream from start_id to get nearest non-extract node ids."""
            origins = set()
            stack = [start_id]
            visited = set()
            while stack:
                current = stack.pop()
                if current in visited:
                    continue
                visited.add(current)
                node = (nodes.get(current) or {})
                if is_extract(node):
                    for up in (node.get("sources", []) or []):
                        stack.append(up)
                else:
                    # Return raw node ids here; map to identifiers later
                    origins.add(current)
            return origins

        # Build reverse adjacency for downstream traversal from a node id
        forward_adj = adjacency  # sources -> node (already built)

        def non_extract_downstream(start_id: str) -> set:
            """Walk forward from start_id through extracts to nearest non-extract target node ids."""
            targets = set()
            stack = [start_id]
            visited = set()
            while stack:
                current = stack.pop()
                if current in visited:
                    continue
                visited.add(current)
                node = (nodes.get(current) or {})
                if is_extract(node):
                    for down in (forward_adj.get(current, []) or []):
                        stack.append(down)
                else:
                    # Return raw node ids here; map to identifiers later
                    targets.add(current)
            return targets

        # Precompute output node ids
        output_node_ids = set([nid for nid, n in nodes.items() if is_output(n)])

        # Helper: get all simple paths from start to any output (skipping extracts transparently)
        def dfs_paths(start: str, target_set: set) -> list:
            all_paths = []
            visited = set()

            def _dfs(curr: str, path: list):
                if curr in visited:
                    return
                path.append(curr)
                visited.add(curr)
                node = (nodes.get(curr) or {})
                # If this is an output, record path
                if curr in target_set:
                    all_paths.append(list(path))
                # Walk forward
                for nxt in forward_adj.get(curr, []):
                    _dfs(nxt, path)
                visited.remove(curr)
                path.pop()

            _dfs(start, [])
            return all_paths

        for src_id, targets in adjacency.items():
            src_candidates = non_extract_upstream(src_id)
            for tgt_id in targets:
                tgt_candidates = non_extract_downstream(tgt_id)
                for true_src in src_candidates:
                    for true_tgt in tgt_candidates:
                        # Map node ids to identifiers (tables -> friendly; transformations -> source_id)
                        src_entity_id = id_map.get(true_src)
                        tgt_entity_id = id_map.get(true_tgt)
                        # Prefer human-friendly entity names where available for relation IDs
                        def _friendly(nid: str) -> str:
                            nd = (nodes.get(nid) or {})
                            if is_load(nd):
                                # Use normalized dataset name
                                ds = ((nd.get("parameters") or {}).get("dataset") or {})
                                nm = ds.get("name", "")
                                return nm if nm else nid
                            if is_output(nd):
                                nm = ((nd.get("parameters") or {}).get("name") or "")
                                return nm if nm else nid
                            return nid
                        # For transformations use already computed source ids; for tables use friendly names
                        if classify(true_src, nodes.get(true_src)) != "transformation":
                            src_entity_id = _friendly(true_src) if src_entity_id == true_src else src_entity_id
                        if classify(true_tgt, nodes.get(true_tgt)) != "transformation":
                            tgt_entity_id = _friendly(true_tgt) if tgt_entity_id == true_tgt else tgt_entity_id
                        if not (src_entity_id and tgt_entity_id):
                            continue
                        # If source is a load node, insert the grouped task id in between
                        if is_load(nodes.get(true_src)):
                            # Find an output end whose path includes the current true_tgt as the next segment
                            chosen_end = None
                            paths = dfs_paths(true_src, output_node_ids)
                            for p in paths:
                                if len(p) >= 2 and p[1] == true_tgt:
                                    chosen_end = p[-1]
                                    break
                            if not chosen_end and paths:
                                chosen_end = paths[0][-1]
                            if chosen_end:
                                task_id = f"task.flow.{str(connection_id)}.{true_src.lower()}__to__{chosen_end.lower()}"
                                # Relation: load -> task
                                key1 = (src_entity_id, task_id)
                                if key1 not in added_pairs:
                                    relations.append({
                                        "srcTableId": src_entity_id,
                                        "tgtTableId": task_id,
                                    })
                                    added_pairs.add(key1)
                                # Column-level relations for load -> task (default 1:1 column names)
                                try:
                                    src_node = nodes.get(true_src) or {}
                                    lp = (src_node.get("parameters") or {})
                                    ds = (lp.get("dataset") or {})
                                    ds_name = ds.get("name", "")
                                    ds_type = ds.get("type") or ds.get("data_type") or ""
                                    # pull explicit fields or fallback to attributes
                                    load_fields = list(lp.get("fields") or [])
                                    if not load_fields:
                                        attrs = __get_sdc_attributes(config, ds_name, ds_type)
                                        load_fields = [str(a.get("name", "")) for a in (attrs or []) if a and a.get("name")]
                                    for col in load_fields:
                                        if not col:
                                            continue
                                        relations.append({
                                            "srcTableId": src_entity_id,
                                            "tgtTableId": task_id,
                                            "srcTableColName": str(col),
                                            "tgtTableColName": str(col),
                                        })
                                except Exception:
                                    pass
                                # Relation: task -> next node
                                key2 = (task_id, tgt_entity_id)
                                if key2 not in added_pairs:
                                    relations.append({
                                        "srcTableId": task_id,
                                        "tgtTableId": tgt_entity_id,
                                    })
                                    added_pairs.add(key2)
                                # Do not add direct load -> next relation
                                continue
                        # Default relation
                        key = (src_entity_id, tgt_entity_id)
                        if key not in added_pairs:
                            relations.append({
                                "srcTableId": src_entity_id,
                                "tgtTableId": tgt_entity_id,
                            })
                            added_pairs.add(key)

        # Add column-level relations for output nodes using fieldsMappings
        column_pairs = set()
        for out_node_id, out_node in nodes.items():
            if not is_output(out_node):
                continue
            mappings = ((out_node or {}).get("parameters", {}) or {}).get("fieldsMappings", []) or []
            if not mappings:
                continue
            # Determine target table id (friendly name) for output node
            tgt_entity_id = id_map.get(out_node_id) or out_node_id
            if classify(out_node_id, out_node) != "transformation":
                tgt_entity_id = _friendly(out_node_id) if tgt_entity_id == out_node_id else tgt_entity_id
            # Choose a source producer for the output:
            # Prefer the immediate upstream transformation if present; else first declared source
            upstreams = (out_node or {}).get("sources", []) or []
            chosen_src = None
            for cand in upstreams:
                if classify(cand, nodes.get(cand)) == "transformation":
                    chosen_src = cand
                    break
            if not chosen_src and upstreams:
                chosen_src = upstreams[0]
            if not chosen_src:
                continue
            src_entity_id = id_map.get(chosen_src) or chosen_src
            if classify(chosen_src, nodes.get(chosen_src)) != "transformation":
                src_entity_id = _friendly(chosen_src) if src_entity_id == chosen_src else src_entity_id
            for m in mappings:
                s_col = str((m or {}).get("sourceField", ""))
                t_col = str((m or {}).get("targetField", ""))
                if not (s_col and t_col):
                    continue
                key = (src_entity_id, tgt_entity_id, s_col, t_col)
                if key in column_pairs:
                    continue
                relations.append({
                    "srcTableId": src_entity_id,
                    "tgtTableId": tgt_entity_id,
                    "srcTableColName": s_col,
                    "tgtTableColName": t_col,
                })
                column_pairs.add(key)

        lineage["tables"] = tables + transformations
        lineage["relations"] = relations
        return lineage
    except Exception as e:
        log_error("Salesforce Data Cloud - Prepare SDC Lineage Failed", e)
        raise e

def __save_sdc_transformations(
    config: dict,
    pipeline_info: dict,
    pipeline_id: str,
    asset: dict,
    lineage: dict,
    all_runs
):
    try:
        connection_data = config.get("connection", {})
        connection_id = connection_data.get("id")
        asset_id = asset.get("id")
        last_run_status = (pipeline_info.get("lastRunStatus") or "").lower()
        last_run_date = pipeline_info.get("lastRunDate")

        # Transformations are those entries in lineage.tables with source_type != Table
        transformations = [t for t in lineage.get("tables", []) if str(t.get("source_type", "")).lower() != "table"]
        if not transformations:
            return

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            # Delete existing SDC transformations for this pipeline
            query_string = f"""
                delete from core.pipeline_transformations
                where asset_id = '{asset_id}' and pipeline_id = '{pipeline_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            last_run_detail = all_runs[0] if all_runs else {}
            last_run_source_id = ""
            if isinstance(last_run_detail, dict):
                last_run_source_id = (
                    last_run_detail.get("job_id")
                    or last_run_detail.get("uniqueId")
                    or ""
                )

            insert_objects = []
            for tf in transformations:
                name = tf.get("name", "")
                params = tf.get("parameters", {})
                trans_source_id = tf.get("source_id") or f"transformation.{str(connection_id)}.{name}.transformation"
                task_id = tf.get("task_id", "")
                props = {
                    "action": tf.get("action", ""),
                    "parameters": params,
                    "sources": tf.get("sources", []),
                    "task_id": task_id,
                }
                query_input = (
                    uuid4(),                # id
                    name,                   # name
                    "",                    # description
                    last_run_source_id,     # run_id (store last run's source_id)
                    json.dumps(props, default=str).replace("'", "''"),
                    trans_source_id,  # source_id (unique with connection and task)
                    pipeline_id,            # pipeline_id
                    "transform",           # source_type
                    asset_id,               # asset_id
                    connection_id,          # connection_id
                    True,                   # is_active
                    False,                  # is_delete
                    datetime.now(),         # created_at
                    datetime.now(),         # updated_at
                    last_run_status,        # status
                    "",                    # error
                    last_run_date,          # run_start_at
                    last_run_date,          # run_end_at
                    0,                      # duration
                    last_run_date,          # last_run_at
                )
                input_literals = ", ".join(["%s"] * len(query_input))
                query_param = cursor.mogrify(
                    f"({input_literals})", query_input
                ).decode("utf-8")
                insert_objects.append(query_param)

            if insert_objects:
                insert_objects = split_queries(insert_objects)
                for input_values in insert_objects:
                    try:
                        query_input = ",".join(input_values)
                        query_string = f"""
                            insert into core.pipeline_transformations(
                                id, name, description, run_id, properties, source_id,
                                pipeline_id, source_type, asset_id, connection_id, is_active, is_delete,
                                created_at, updated_at, status, error, run_start_at, run_end_at,
                                duration, last_run_at
                            ) values {query_input}
                        """
                        cursor = execute_query(connection, cursor, query_string)
                    except Exception as e:
                        log_error("Salesforce Data Cloud: inserting transformations", e)
    except Exception as e:
        log_error("Salesforce Data Cloud - Save Transformations Failed", e)
        raise e

def __get_response(config: dict, url: str = "", method_type: str = "get", params=None, method_name="execute"):
    api_response = None
    
    try:
        pg_connection = get_postgres_connection(config)
        api_response = agent_helper.execute_query(
            config,
            pg_connection,
            "",
            method_name="execute",
            parameters=dict(
                method_name=method_name,
                request_type=method_type,
                request_params=params
            ),
        )
        
        api_response = api_response if api_response else {}
        
        return api_response
    except Exception as e:
        raise e


def __get_sdc_attributes(config: dict, table_name: str, asset_type: str) -> list:
    """
    Fetch attribute metadata (field name and type) for a given SDC dataset/object.
    Expects API to return a list of objects with at least 'name' and 'type' keys.
    """
    try:
        if not table_name:
            return []
        params = {
            "table_name": table_name,
            "asset_type": asset_type,
        }
        resp = __get_response(
            config,
            method_type="get",
            params=params,
            method_name="get_pipeline_attributes",
        )
        if isinstance(resp, list):
            return resp
        if isinstance(resp, dict):
            # Support nested payloads if returned as { attributes: [...] }
            for key in ["attributes", "fields", "data"]:
                val = resp.get(key)
                if isinstance(val, list):
                    return val
        return []
    except Exception:
        return []
def _status_to_human(value):
    if value is None:
        return ""
    v = str(value).strip().lower()
    if v in ["1", "completed", "complete", "success", "succeeded"]:
        return "success"
    if v in ["0", "error", "failed", "failure"]:
        return "failed"
    if v in ["cancelled", "canceled"]:
        return "cancelled"
    if v in ["2", "running", "active"]:
        return "running"
    return v

def __save_pipeline_runs(
    config: dict,
    data: dict,
    asset_id: str,
    pipeline_id: str,
    pipeline_type: str = "pipeline",
    runs: list = []
) -> None:
    """
    Save / Update Runs For Pipeline
    """
    all_runs = []
    latest_run = False
    try:
        connection_data = config.get("connection", {})
        connection_id = connection_data.get("id")
        connection = get_postgres_connection(config)
        
        if not connection_id or not runs:
            return
        pipeline_runs = []
        all_pipeline_runs = []
        with connection.cursor() as cursor:
            # Clear Existing Runs Details
            query_string = f"""
                delete from core.pipeline_runs_detail
                where
                    asset_id = '{asset_id}'
                    and pipeline_id = '{pipeline_id}'
            """
            cursor = execute_query(connection, cursor, query_string)

            insert_objects = []
            unique_run_id = str(uuid4())
            for i, item in enumerate(runs):
                started_at = item.get("startTime") if item.get("startTime") else None
                finished_at = item.get("endTime") if item.get("endTime") else item.get("startTime")
                if pipeline_type == "job":
                    started_at = datetime.fromtimestamp(item.get("startTime") / 1000) if item.get("startTime") else None
                    finished_at = datetime.fromtimestamp(item.get("endTime") / 1000) if item.get("endTime") else None

                run_id =  str(uuid4())
                name_value = item.get("definitionName") or item.get("name") or item.get("Name") or f"run_{i}"
                safe_name = str(name_value).lower().replace(" ", "_")
                # Build deterministic source_id to avoid duplicates across pulls
                ts_basis = started_at or finished_at or f"{i}"
                safe_ts = str(ts_basis).replace(":", "").replace("-", "").replace("Z", "").replace(".", "").replace("+00:00", "").replace("T", "t")
                source_id = f"{pipeline_type}.{asset_id}.{safe_ts}"


                status_humanized = _status_to_human(item.get("StatusMessage")) or _status_to_human(item.get("status"))
                error_message = item.get("errorMessage", "").replace("'", "''").lower()
                duration = None
                if started_at and finished_at:
                    # Parse the string timestamps to datetime objects
                    started_datetime = datetime.fromisoformat(started_at.replace('Z', '+00:00'))
                    finished_datetime = datetime.fromisoformat(finished_at.replace('Z', '+00:00'))
                    
                    # Now you can subtract them
                    time_delta = finished_datetime - started_datetime
                    duration = time_delta.total_seconds()
                
                individual_run = {
                    "id": run_id,
                    "runGeneratedAt":started_at,
                    "status": item.get("status", item.get("status", "")),
                    "status_humanized": status_humanized,
                    "status_message": item.get("StatusMessage", ""),
                    "created_at": item.get("startTime", datetime.now()),
                    "updated_at": item.get("endTime", ""),
                    "started_at": started_at,
                    "finished_at": finished_at,
                    "job": item.get("job", ""),
                    "environment": item.get("environment", ""),
                    "run_steps": item.get("run_steps", ""),
                    "in_progress": item.get("in_progress", ""),
                    "is_complete": True if item.get("StatusMessage", "").lower() == "complete" else False,
                    "is_success": True if status_humanized == "success" else False, 
                    "is_error": True if status_humanized == "failed" else False,
                    "is_cancelled": item.get("is_cancelled", ""),
                    "duration": duration,
                    "run_duration":duration,
                    "error_message": error_message,
                    "job_id": source_id,
                    "unique_id": source_id,
                    "is_running": item.get("is_running", ""),
                    "href": item.get("href", ""),
                    "uniqueId": source_id,
                    "unique_job_id": item.get("job_id", ""),
                    "models": [
                        {**each_activity, "uniqueId": source_id}
                        for each_activity in item.get("dataflow_activities", [])
                    ],
                }

                # make properties
                properties = individual_run

                # Validating existing runs (idempotent upsert by deterministic source_id)
                query_string = f"""
                    select id from core.pipeline_runs
                    where
                        asset_id = '{asset_id}'
                        and pipeline_id = '{pipeline_id}'
                        and source_id = '{individual_run.get('job_id')}'
                """
                cursor = execute_query(connection, cursor, query_string)
                existing_run = fetchone(cursor)
                existing_run = existing_run.get("id") if existing_run else None
                
                if existing_run:
                    query_string = f"""
                        update core.pipeline_runs set 
                            status = '{status_humanized}', 
                            error = '{error_message}',
                            run_start_at = {f"'{started_at}'" if started_at else 'NULL'},
                            run_end_at = {f"'{finished_at}'" if finished_at else 'NULL'},
                            duration = '{duration}' ,
                            properties = '{json.dumps(properties, default=str).replace("'", "''")}'
                        where id = '{existing_run}'
                    """
                    cursor = execute_query(connection, cursor, query_string)
                    pipeline_runs.append(
                        {
                            "id": existing_run,
                            "source_id": individual_run.get("job_id"),
                            "is_update": True,
                        }
                    )
                    all_pipeline_runs.append(
                        {
                            "id": existing_run,
                            "source_id": individual_run.get("job_id"),
                            "is_update": True,
                        }
                    )
                else:
                    query_input = (
                        run_id,
                        individual_run.get("job_id"),
                        str(uuid4()),
                        status_humanized,
                        error_message,
                        individual_run.get('started_at') if individual_run.get('started_at') else None,
                        individual_run.get('finished_at') if individual_run.get('finished_at') else None,
                        duration,
                        json.dumps(properties, default=str).replace("'", "''"),
                        True,
                        False,
                        pipeline_id,
                        asset_id,
                        connection_id,
                    )
                    all_pipeline_runs.append(query_input)
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals})", query_input
                    ).decode("utf-8")
                    insert_objects.append(query_param)
                    if not latest_run:
                        latest_run = True
                all_runs.append(individual_run)

            insert_objects = split_queries(insert_objects)

            for input_values in insert_objects:
                try:
                    query_input = ",".join(input_values)
                    query_string = f"""
                        insert into core.pipeline_runs(
                            id, source_id, technical_id, status, error,  run_start_at, run_end_at, duration,
                            properties, is_active, is_delete, pipeline_id, asset_id, connection_id
                        ) values {query_input} 
                        RETURNING id, source_id;
                    """
                    
                    cursor = execute_query(connection, cursor, query_string)
                    pipeline_runs = fetchall(cursor)
                except Exception as e:
                    log_error("Salesforce Data Cloud Runs Insert Failed  ", e)
            # save individual run detail
            __save_runs_details(config, all_runs, pipeline_id, pipeline_runs)
            return all_pipeline_runs
    except Exception as e:
        log_error(f"Salesforce Data Cloud Pipeline Connector - Saving Runs Failed ", e)
    finally:
        return latest_run, all_runs


def __get_pipeline_id(config: dict) -> str:
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select id from core.pipeline
                where asset_id = '{asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            pipeline = fetchone(cursor)
            if pipeline:
                return pipeline.get("id")
    except Exception as e:
        log_error(
            f"Salesforce Data Cloud Connector - Get Pipeline Primary Key Information By Asset ID Failed ",
            e,
        )
        raise e

def __save_pipeline_tasks(config: dict, pipeline_info: dict, pipeline_id:str, asset_id: str, pipeline_properties: dict = {},  pipeline_type: str = "task", all_runs = []):
    """
    Save / Update Task For Pipeline
    """
    new_pipeline_tasks = []
    try:
        connection_data = config.get("connection", {})
        connection_id = connection_data.get("id")
        connection = get_postgres_connection(config)
        # Salesforce Data Cloud definition-based tasks
        if isinstance(pipeline_info.get("definition", {}).get("nodes"), dict):
           new_pipeline_tasks=  __save_sdc_pipeline_tasks(
                config=config,
                pipeline_info=pipeline_info,
                pipeline_id=pipeline_id,
                asset_id=asset_id,
                pipeline_properties=pipeline_properties,
            )

    except Exception as e:
        log_error(f"Salesforce Data Cloud Pipeline Connector - Saving tasks ", e)
        raise e
    finally:
        return new_pipeline_tasks


def __get_pipeline_detail(config: dict, asset_properties: dict, credentials: dict):
    try:
        if asset_properties.get("unique_id") == "":
            return {}
        # automation detail url
        api_response = __get_response(
            config,
            "get",
            {"pipeline_id": asset_properties.get("pipeline_id")},
            method_name="get_pipeline_detail"
        )
        return api_response
    except Exception as e:
        raise e


def __save_sdc_pipeline_tasks(
    config: dict,
    pipeline_info: dict,
    pipeline_id: str,
    asset_id: str,
    pipeline_properties: dict,
):
    new_pipeline_tasks = []
    try:
        connection_data = config.get("connection", {})
        connection_id = connection_data.get("id")
        connection = get_postgres_connection(config)
        nodes = (pipeline_info.get("definition", {}) or {}).get("nodes", {})
        last_run_status = (pipeline_info.get("lastRunStatus") or "").lower()
        last_run_status = _status_to_human(last_run_status)
        last_run_error = (pipeline_info.get("lastRunErrorMessage") or "").lower()
        if not isinstance(nodes, dict) or not nodes:
            return []

        # Build adjacency from sources -> node
        adjacency = {}
        reverse_sources = {}
        for node_id, node in nodes.items():
            sources = (node or {}).get("sources", []) or []
            reverse_sources[node_id] = list(sources)
            for src in sources:
                adjacency.setdefault(src, []).append(node_id)

        def is_load(nid: str) -> bool:
            return ((nodes.get(nid) or {}).get("action", "").strip().lower() == "load")

        def is_output(nid: str) -> bool:
            a = (nodes.get(nid) or {}).get("action", "").strip().lower()
            return a in ["outputd360", "output", "outputdataset"]

        load_nodes = [nid for nid in nodes.keys() if is_load(nid)]
        output_nodes = set([nid for nid in nodes.keys() if is_output(nid)])

        # DFS to find all simple paths from each load to each output
        def dfs_paths(start: str, target_set: set) -> list:
            all_paths = []
            max_len = len(nodes) + 1

            def _dfs(curr: str, path: list, visited: set):
                if len(path) > max_len:
                    return
                if curr in visited:
                    return
                path.append(curr)
                visited.add(curr)
                if curr in target_set:
                    all_paths.append(list(path))
                for nxt in adjacency.get(curr, []):
                    _dfs(nxt, path, visited)
                visited.remove(curr)
                path.pop()

            _dfs(start, [], set())
            return all_paths

        # Compose flow specs: one per unique (load -> output) path
        flows = []
        for load_id in load_nodes:
            paths = dfs_paths(load_id, output_nodes)
            for path in paths:
                start = load_id
                end = path[-1]
                # Edges along the path
                edges = []
                for i in range(len(path) - 1):
                    edges.append({"source": path[i], "target": path[i + 1]})
                # Actions list for quick view
                actions = [str((nodes.get(n) or {}).get("action", "")).strip() for n in path]
                # Flow property payload
                flow_prop = {
                    **pipeline_properties,
                    "group_type": "flow",
                    "start_node": start,
                    "end_node": end,
                    "node_ids": path,
                    "edges": edges,
                    "actions": actions,
                    "nodes": {nid: nodes.get(nid) for nid in path},
                }
                # Deterministic source_id to ensure stable upsert; include connection for cross-connection uniqueness
                source_id = f"task.flow.{str(connection_id)}.{start.lower()}__to__{end.lower()}"
                flows.append({
                    "source_id": source_id,
                    "properties": flow_prop,
                })

        # If no flows detected, fallback to no-op
        if not flows:
            return []

        with connection.cursor() as cursor:
            # Clean previously grouped flow tasks for this asset

            # Insert/Update flows with dynamic names task1, task2, ...
            batch = []
            for idx, flow in enumerate(flows, start=1):
                source_id = flow["source_id"]
                name = f"task{idx}"
                properties = {
                    **(flow["properties"] or {}),
                    "name": name,
                }

                # Upsert: check existing by asset_id + source_id
                query_string = f"""
                    select id from core.pipeline_tasks
                    where asset_id = '{asset_id}' and source_id = '{source_id}'
                """
                cursor = execute_query(connection, cursor, query_string)
                existing_task = fetchone(cursor)
                existing_task = existing_task.get("id") if existing_task else None

                if existing_task:
                    query_string = f"""
                        update core.pipeline_tasks set 
                            properties = '{json.dumps(properties, default=str).replace("'", "''")}',
                            updated_at = '{datetime.now()}',
                            source_type = 'task',
                            name = '{name}',
                            status = '{last_run_status}',
                            error = '{last_run_error}'
                        where id = '{existing_task}'
                    """
                    cursor = execute_query(connection, cursor, query_string)
                    new_pipeline_tasks.append(existing_task)
                else:
                    pipeline_task_id = str(uuid4())
                    query_input = (
                        pipeline_task_id,
                        source_id,
                        name,
                        "",
                        json.dumps(properties, default=str).replace("'", "''"),
                        None,
                        None,
                        'task',
                        str(pipeline_id),
                        str(asset_id),
                        str(connection_id),
                        True,
                        True,
                        False,
                        datetime.now(),
                        "",
                        last_run_status,
                        last_run_error
                    )
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(
                        f"({input_literals})", query_input
                    ).decode("utf-8")
                    batch.append(query_param)
                    new_pipeline_tasks.append(pipeline_task_id)

            if batch:
                inputs_split = split_queries(batch)
                for input_values in inputs_split:
                    try:
                        query_input = ",".join(input_values)
                        tasks_insert_query = f"""
                            insert into core.pipeline_tasks (id, source_id, name, owner, properties, run_start_at,
                            run_end_at, source_type, pipeline_id, asset_id,connection_id, is_selected, is_active, is_delete,
                            created_at, description, status,error)
                            values {query_input}
                            RETURNING id, source_id;
                        """
                        cursor = execute_query(connection, cursor, tasks_insert_query)
                        _ = fetchall(cursor)
                    except Exception as e:
                        log_error("Salesforce Data Cloud: inserting grouped flow tasks", e)

            # Build and save columns for each grouped flow task (load + outputD360)
            try:
                # Fetch all grouped flow tasks for mapping source_id -> id
                map_query = f"""
                    select id, source_id from core.pipeline_tasks
                    where asset_id = '{asset_id}' and pipeline_id = '{pipeline_id}'
                    and properties ->> 'group_type' = 'flow'
                """
                cursor = execute_query(connection, cursor, map_query)
                pipeline_tasks_map = fetchall(cursor) or []

                # Prepare columns from load fields and output object fields
                def _get_output_object_by_name(name: str):
                    try:
                        outputs = (pipeline_info.get('definition', {}) or {}).get('outputDataObjects', []) or []
                        for obj in outputs:
                            if (obj or {}).get('name') == name:
                                return obj
                    except Exception:
                        return None
                    return None

                flow_columns = []
                for flow in flows:
                    props = flow.get('properties', {}) or {}
                    node_ids = props.get('node_ids', []) or []
                    nodes_in_path = props.get('nodes', {}) or {}
                    if not node_ids:
                        continue
                    start_node = props.get('start_node')
                    end_node = props.get('end_node')
                    load_node = (nodes_in_path.get(start_node) or {})
                    output_node = (nodes_in_path.get(end_node) or {})

                    # Load fields (enrich with attributes/types)
                    load_params = (load_node or {}).get('parameters', {}) or {}
                    dataset_info = (load_params.get('dataset') or {})
                    dataset_name = dataset_info.get('name', '')
                    dataset_type = dataset_info.get('type') or dataset_info.get('data_type') or ''
                    load_attrs = __get_sdc_attributes(config, dataset_name, dataset_type)
                    load_attr_map = {str(a.get('name','')): a for a in (load_attrs or [])}
                    load_fields = load_params.get('fields') or []
                    for field_name in load_fields:
                        if not field_name:
                            continue
                        flow_columns.append({
                            'task_source_id': flow['source_id'],
                            'name': str(field_name),
                            'data_type': str((load_attr_map.get(str(field_name)) or {}).get('data_type','')),
                            'properties': {
                                **pipeline_properties,
                                'source': 'load',
                                'dataset_name': dataset_name,
                                'node_id': start_node,
                            }
                        })
                    # If no explicit fields on the load node, fallback to attributes list
                    if not load_fields and load_attrs:
                        for a in load_attrs:
                            col_name = str(a.get('name', ''))
                            if not col_name:
                                continue
                            flow_columns.append({
                                'task_source_id': flow['source_id'],
                                'name': col_name,
                                'data_type': str(a.get('data_type', '')),
                                'properties': {
                                    **pipeline_properties,
                                    'source': 'load',
                                    'dataset_name': dataset_name,
                                    'node_id': start_node,
                                }
                            })

                    # Output fields (lookup by output object's name)
                    output_params = (output_node or {}).get('parameters', {}) or {}
                    output_name = output_params.get('name', '')
                    output_obj = _get_output_object_by_name(output_name)
                    if output_obj:
                        for f in (output_obj.get('fields') or []):
                            flow_columns.append({
                                'task_source_id': flow['source_id'],
                                'name': str(f.get('name', '')),
                                'data_type': str(f.get('type', '')),
                                'properties': {
                                    **pipeline_properties,
                                    'source': 'output',
                                    'object_id': output_obj.get('id', ''),
                                    'object_label': output_obj.get('label', ''),
                                    'object_name': output_obj.get('name', ''),
                                    'is_primary_key': f.get('isPrimaryKey', False),
                                    'key_qualifier_field': f.get('keyQualifierField', ''),
                                    'node_id': end_node,
                                }
                            })

                if flow_columns:
                    __save_sdc_flow_columns(config, flow_columns, pipeline_id, pipeline_tasks_map)
            except Exception as e:
                log_error("Salesforce Data Cloud: saving grouped flow columns failed", e)
            # Create default task measures for newly created/updated tasks (ADF-style)
            try:
                if new_pipeline_tasks:
                    create_pipeline_task_measures(config, new_pipeline_tasks)
            except Exception as e:
                log_error("Salesforce Data Cloud: create task default measures", e)
        return new_pipeline_tasks
    except Exception as e:
        log_error("Salesforce Data Cloud - Save Node Tasks Failed", e)
        raise e

def get_data_by_key(data, value, get_key="id", filter_key="source_id"):
    return next((item for item in data if item[filter_key] == value), None)

def __save_runs_details(
    config: dict, data: list, pipeline_id: str, pipeline_runs: list = None
):
    try:
        connection_obj = config.get("connection", {})
        connection_id = connection_obj.get("id")
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        insert_objects = []
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            # Fetch all tasks for this pipeline to create per-task run details
            tasks_query = f"""
                select id, source_id, name from core.pipeline_tasks
                where asset_id = '{asset_id}' and pipeline_id = '{pipeline_id}'
            """
            cursor = execute_query(connection, cursor, tasks_query)
            pipeline_tasks = fetchall(cursor) or []

            for run in data:
                # Align with DBT pattern:
                # - run_id (in runs_detail) should be the vendor run source_id (pipeline_runs.source_id)
                #   which we store in run['uniqueId'] for SDC
                run_source_id = run.get("uniqueId") or run.get("job_id") or ""
                # Resolve pipeline_run_id FK by looking up the run row we inserted/updated
                pipeline_run_db_id = None
                if run_source_id:
                    lookup_query = f"""
                        select id from core.pipeline_runs
                        where asset_id = '{asset_id}' and pipeline_id = '{pipeline_id}' and source_id = '{run_source_id}'
                        limit 1
                    """
                    cursor = execute_query(connection, cursor, lookup_query)
                    found = fetchone(cursor)
                    pipeline_run_db_id = found.get("id") if found else None
                if not pipeline_run_db_id:
                    # Skip if the parent pipeline_run row is not present to avoid FK violation
                    continue
                status_humanized = get_pipeline_status(run.get("status"))
                error_message = run.get("error_message", "").replace("'", "''")
                started_at = run.get("started_at", None)
                finished_at = run.get("finished_at", None)
                duration = run.get("duration", None)

                # Optional: keep a run-level detail row (source_id == run_source_id)
                base_query_input = (
                    uuid4(),
                    run_source_id,                 # run_id
                    run_source_id,                 # source_id (run-level)
                    "job",
                    run.get("job_id"),
                    status_humanized,
                    error_message,
                    (
                        run.get("rawSql", "").replace("'", "''")
                        if run.get("rawSql")
                        else ""
                    ),
                    (
                        run.get("compiledSql", "").replace("'", "''")
                        if run.get("compiledSql")
                        else ""
                    ),
                    started_at,
                    finished_at,
                    duration,
                    True,
                    False,
                    pipeline_run_db_id,         # pipeline_run_id (FK to core.pipeline_runs.id)
                    pipeline_id,
                    asset_id,
                    connection_id,
                )
                input_literals = ", ".join(["%s"] * len(base_query_input))
                query_param = cursor.mogrify(
                    f"({input_literals})", base_query_input
                ).decode("utf-8")
                insert_objects.append(query_param)

                # Per-task run details: source_id must equal the task's source_id
                for task in pipeline_tasks:
                    task_source_id = task.get("source_id")
                    task_name = task.get("name") or task_source_id
                    task_query_input = (
                        uuid4(),
                        run_source_id,            # run_id (task-specific to avoid collisions)
                        task_source_id,            # source_id (task source id)
                        "task",
                        task_name,
                        status_humanized,
                        error_message,
                        "",
                        "",
                        started_at,
                        finished_at,
                        duration,
                        True,
                        False,
                        pipeline_run_db_id,     # pipeline_run_id (FK to core.pipeline_runs.id)
                        pipeline_id,
                        asset_id,
                        connection_id,
                    )
                    task_literals = ", ".join(["%s"] * len(task_query_input))
                    task_param = cursor.mogrify(
                        f"({task_literals})", task_query_input
                    ).decode("utf-8")
                    insert_objects.append(task_param)

            insert_objects = split_queries(insert_objects)
            for input_values in insert_objects:
                try:
                    query_input = ",".join(input_values)
                    query_string = f"""
                        insert into core.pipeline_runs_detail(
                            id, run_id, source_id, type, name, status, error, source_code, compiled_code,
                            run_start_at, run_end_at, duration, is_active, is_delete,
                            pipeline_run_id, pipeline_id, asset_id, connection_id
                        ) values {query_input} 
                    """
                    cursor = execute_query(connection, cursor, query_string)
                except Exception as e:
                    log_error("Salesforce Data Cloud Pipeline Runs Details Insert Failed  ", e)
    except Exception as e:
        log_error(f"Salesforce Data Cloud Pipeline Connector - Save Runs Details By Run ID Failed ", e)
        raise e

def __save_sdc_flow_columns(
    config: dict,
    columns: list,
    pipeline_id: str,
    pipeline_tasks_map: list,
):
    try:
        connection_obj = config.get("connection", {})
        connection_id = connection_obj.get("id")
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        # Build source_id -> pipeline_task_id map
        task_map = {}
        for row in (pipeline_tasks_map or []):
            src = row.get("source_id")
            tid = row.get("id")
            if src and tid:
                task_map[src] = tid

        if not columns:
            return

        # Collect target pipeline_task_ids for cleanup
        target_task_ids = list({task_map.get(c.get("task_source_id")) for c in columns if task_map.get(c.get("task_source_id"))})

        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            if target_task_ids:
                ids_literal = ",".join([f"'{x}'" for x in target_task_ids])
                cleanup_query = f"""
                    delete from core.pipeline_columns
                    where asset_id = '{asset_id}' and pipeline_id = '{pipeline_id}'
                    and pipeline_task_id in ({ids_literal})
                """
                cursor = execute_query(connection, cursor, cleanup_query)

            insert_objects = []
            for col in columns:
                pipeline_task_id = task_map.get(col.get("task_source_id"))
                if not pipeline_task_id:
                    continue
                name = col.get("name")
                data_type = col.get("data_type") or ""
                properties = col.get("properties") or {}
                query_input = (
                    uuid4(),
                    name,
                    "",
                    None,
                    data_type,
                    [],
                    True,
                    False,
                    pipeline_task_id,
                    pipeline_id,
                    asset_id,
                    connection_id,
                    json.dumps(properties, default=str).replace("'", "''"),
                    None,
                )
                input_literals = ", ".join(["%s"] * len(query_input))
                query_param = cursor.mogrify(
                    f"({input_literals})", query_input
                ).decode("utf-8")
                insert_objects.append(query_param)

            if insert_objects:
                inputs_split = split_queries(insert_objects)
                for input_values in inputs_split:
                    try:
                        query_input = ",".join(input_values)
                        query_string = f"""
                            insert into core.pipeline_columns(
                                id, name, description,  comment, data_type, tags,
                                is_active, is_delete, pipeline_task_id, pipeline_id, asset_id, connection_id,
                                properties, lineage_entity_id
                            ) values {query_input} 
                        """
                       
                        cursor = execute_query(connection, cursor, query_string)
                    except Exception as e:
                        log_error("Salesforce Data Cloud: inserting flow columns", e)
    except Exception as e:
        log_error("Salesforce Data Cloud - Save Flow Columns Failed", e)
        raise e

def __prepare_search_key(data: dict) -> dict:
    search_keys = ""
    try:
        keys = {
            "id": data.get("id", ""),
            "name": data.get("name", ""),
            "description": data.get("description", "")
        }
        keys = keys.values()
        keys = [str(x) for x in keys if x]

        if len(keys) > 0:
            search_keys = " ".join(keys)
    except Exception as e:
        log_error(f"Salesforce Data Cloud Connector - Prepare Search Keys ", e)
    finally:
        return search_keys


def __prepare_description(asset, asset_type=None):
    job_description = asset.get('description',"")
    job_description = f"{job_description}".strip()

    if job_description and job_description != "None" and job_description != "":
        return job_description
    
    # Handle different types of assets
    if asset_type == "dashboards":
        # For dashboards, asset is already the dashboardMetadata object
        name = asset.get("name", "")
        owner = asset.get("owner", {})
        owner_name = owner.get("displayName", "") if owner else ""
        runGeneratedAt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp for dashboards
        
        generated_description = f"""This Salesforce dashboard {name} is part of Salesforce Data Cloud. 
        Owned by {owner_name} and runs generated at {runGeneratedAt}"""
        
    elif asset_type == "reports":        # For reports, asset is already the reportMetadata object
        name = asset.get("name", "")
        owner_name = "System"  # Reports don't have owner info in the structure
        runGeneratedAt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Current timestamp for reports
        
        generated_description = f"""This Salesforce report {name} is part of Salesforce Data Cloud. runs generated at {runGeneratedAt}"""

        
    else:
        # Original logic for existing functionality when type is None
        properties = asset.get("properties")
        name = properties.get("name", "")
        project_name = properties.get("project_name", "")
        group_name = properties.get("group_name", "")
        owner_name = properties.get("owner_name", "")
        runGeneratedAt = properties.get("runGeneratedAt", "")

        generated_description = f"""This Salesforce pipeline {name} is part of Project {project_name} and group {group_name}. 
        Owned by {owner_name} and runs generated at {runGeneratedAt}"""    
    return f"{generated_description}".strip()

def __update_pipeline_stats(config: dict) -> str:
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:

            # Get Last Run Info
            query_string = f"""
                select
                    id,
                    source_id,
                    run_end_at,
                    duration,
                    status
                from
                    core.pipeline_runs
                where asset_id = '{asset_id}'
                order by run_end_at desc
                limit 1
            """
            cursor = execute_query(connection, cursor, query_string)
            last_run = fetchone(cursor)
            last_run = last_run if last_run else {}

            # Get Pipeline Stats
            query_string = f"""
                SELECT 
                pipeline.id,
                COUNT(DISTINCT pipeline_columns.id) as tot_columns,
                COUNT(DISTINCT pipeline_runs.id) as tot_runs,
                COUNT(DISTINCT pipeline_transformations.id) as tot_transformations
            FROM core.pipeline
            LEFT JOIN core.pipeline_columns ON pipeline_columns.asset_id = pipeline.asset_id
            LEFT JOIN core.pipeline_runs ON pipeline_runs.asset_id = pipeline.asset_id
            LEFT JOIN core.pipeline_transformations ON pipeline_transformations.asset_id = pipeline.asset_id
            WHERE pipeline.asset_id = '{asset_id}'
            GROUP BY pipeline.id;
            """
            cursor = execute_query(connection, cursor, query_string)
            pipeline_stats = fetchone(cursor)
            pipeline_stats = pipeline_stats if pipeline_stats else {}

            # Update Pipeline Status
            query_string = f"""
                SELECT id, properties FROM core.pipeline
                WHERE asset_id = '{asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            report = fetchone(cursor)
            
            if report:
                properties = report.get("properties", {})
                properties.update({
                    "tot_columns": pipeline_stats.get("tot_columns", 0),
                    "tot_runs": pipeline_stats.get("tot_runs", 0),
                    "tot_transformations": pipeline_stats.get("tot_transformations",0)
                })

                run_id = last_run.get("source_id", "")
                status = last_run.get("status", "")
                last_run_at = last_run.get("run_end_at")
                
                query_string = f"""
                    UPDATE core.pipeline SET 
                        run_id = '{run_id}', 
                        status = '{status}', 
                        last_run_at = {f"'{last_run_at}'" if last_run_at else 'NULL'},
                        properties = '{json.dumps(properties, default=str).replace("'", "''")}'
                    WHERE asset_id = '{asset_id}'
                """
                cursor = execute_query(connection, cursor, query_string)
    except Exception as e:
        log_error(f"Salesforce Data Cloud Connector - Update Run Stats to Job Failed ", e)
        raise e


def __extract_sdc_pipeline_measure(config: dict):
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        connection = get_postgres_connection(config)
        
        # Fetch pipeline_name (job_name) from pipeline table
        pipeline_name = None
        if asset_id:
            with connection.cursor() as cursor:
                pipeline_query = f"""
                    select p.name as pipeline_name
                    from core.pipeline p
                    where p.asset_id = '{asset_id}'
                    limit 1
                """
                cursor = execute_query(connection, cursor, pipeline_query)
                pipeline_result = fetchone(cursor)
                pipeline_name = pipeline_result.get("pipeline_name") if pipeline_result else None
        
        # Pull the latest two pipeline runs for duration deltas
        with connection.cursor() as cursor:
            runs_query = f"""
                select source_id, run_start_at as started_at, run_end_at as finished_at, duration, id
                from core.pipeline_runs
                where asset_id = '{asset_id}'
                order by run_start_at desc
                limit 2
            """
            cursor = execute_query(connection, cursor, runs_query)
            runs = fetchall(cursor) or []

        if not runs:
            return
        # Build job (asset) measure
        latest = runs[0]
        previous = runs[1] if len(runs) > 1 else {}
        job_measure = {
            "duration": latest.get("duration"),
            "last_run_date": latest.get("started_at"),
            "previous_run_date": previous.get("started_at") if previous else None,
        }
        # Pass pipeline_name (job_name) for asset level measures
        execute_pipeline_measure(config, "asset", job_measure, job_name=pipeline_name)

        # Task measures: fetch tasks and use their last run timestamps from pipeline_runs_detail
        with connection.cursor() as cursor:
            tasks_query = f"""
                select id as task_id, source_id, name from core.pipeline_tasks
                where asset_id = '{asset_id}' and is_active = true and is_delete = false
            """
            cursor = execute_query(connection, cursor, tasks_query)
            tasks = fetchall(cursor) or []
            

        if not tasks:
            return

        for task in tasks:
            source_id = task.get("source_id")
            task_name = task.get("name")
            # Get latest two run details for this task
            with connection.cursor() as cursor:
                detail_query = f"""
                    select run_start_at as started_at, run_end_at as finished_at, duration
                    from core.pipeline_runs_detail
                    where asset_id = '{asset_id}' and source_id = '{source_id}'
                    order by run_start_at desc
                    limit 2
                """
                
                cursor = execute_query(connection, cursor, detail_query)
                details = fetchall(cursor) or []
            if not details:
                continue
            latest_d = details[0]
            previous_d = details[1] if len(details) > 1 else {}
            task_measure = {
                "duration": latest_d.get("duration"),
                "last_run_date": latest_d.get("started_at"),
                "previous_run_date": previous_d.get("started_at") if previous_d else None,
            }
            
            # Pass pipeline_name (job_name) and task_name for task level measures
            execute_pipeline_measure(config, "task", task_measure, task_info=task, job_name=pipeline_name, task_name=task_name)
    except Exception as e:
        log_error("Salesforce Data Cloud - extract measures", e)

def __get_assets_by_name_and_connection(config, table_name: str, connection_type: str = 'salesforce_data_cloud') -> list:
    assets = []
    try:
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            query_string = f"""
                select asset.id, asset.name from core.asset
                join core.connection on connection.id = asset.connection_id
                where asset.is_active = true and asset.is_delete = false
                and lower(connection.type) = lower('{connection_type}')
                and asset.name = '{table_name}'
                order by asset.created_date desc
                limit 1
            """
            cursor = execute_query(connection, cursor, query_string)
            assets = fetchall(cursor)
    except Exception as e:
        log_error("SDC Get Asset By Name and Connection Failed ", e)
    finally:
        return assets

def __map_asset_with_lineage(config, lineage):
    try:
        lineage_tables = lineage.get("tables", []) or []
        if not lineage_tables:
            return
        for table in lineage_tables:
            action = str(table.get("action", "")).lower()
            if action not in ["load", "outputd360", "output", "outputdataset"]:
                continue
            entity_name = table.get("entity_name") or table.get("name") or ""
            if not entity_name:
                continue
            assets = __get_assets_by_name_and_connection(config, entity_name, 'salesforce_data_cloud')
            if assets:
                save_asset_lineage_mapping(config, "pipeline", table, assets, True)
    except Exception as e:
        log_error("Error in mapping SDC asset with lineage", e)




def extract_salesforce_data_cloud_reports(config, **kwargs):
    """
    Main method to extract Salesforce Data Cloud reports
    Similar to extract_tableau_data but for Salesforce Data Cloud reports
    """
    try:
        is_completed = check_task_status(config, kwargs)
        if is_completed:
            return

        global TASK_CONFIG
        task_config = get_task_config(config, kwargs)
        run_id = config.get("run_id")
        connection = config.get("connection", {})
        dag_info = config.get("dag_info")
        asset = config.get("asset", {})
        asset_id = asset.get("id")

        update_queue_detail_status(
            config, ScheduleStatus.Running.value, task_config=task_config
        )
        update_queue_status(config, ScheduleStatus.Running.value, True)

        asset_properties = asset.get("properties")
        credentials = connection.get("credentials")
        connection_type = connection.get("type", "")
        credentials = decrypt_connection_config(credentials, connection_type)
        TASK_CONFIG = config

        # Validate connection and get authentication token
        asset_type = config.get("type", "reports").lower()
        report_id = asset_properties.get("report_id")
        type = config.get("type","").lower()
        
        # Get data from Salesforce Data Cloud (works for both reports and dashboards)
        data = __get_report_detail(config, report_id,type, credentials)
        if data:
            # Save data for both reports and dashboards (includes asset statistics)            
            # Save columns for both reports and dashboards
            save_report_columns(config, data)
            save_reports_data(config, data)
            # Prepare lineage for both reports and dashboards
            __prepare_reports_lineage(config, data)
            update_reports_propagations(config, config.get("asset", {}))
                    

        # Update request queue status
        is_triggered = config.get("is_triggered")
        if not is_triggered:
            create_queue_detail(config)
        update_queue_detail_status(config, ScheduleStatus.Completed.value)
    except Exception as e:
        log_error("Salesforce Data Cloud Reports Pull Failed", e)
        update_queue_detail_status(config, ScheduleStatus.Failed.value, str(e))
    finally:
        update_report_last_runs(config)


def __get_report_detail(config: dict, report_id: str, type: str, credentials: dict):
    try:
        api_response = __get_response(
            config,
            "get",
            {"type": type},
            {"report_id": report_id},
            method_name="get_report_detail"
        )
        return api_response
    except Exception as e:
        raise e




def save_reports_data(config: dict, data: dict):
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        asset_properties = asset.get("properties")
        connection_data = config.get("connection", {})
        connection_id = connection_data.get("id")
        
        # Check asset type from config
        asset_type = config.get("type", "reports").lower()
        if asset_type == "dashboards":
            # Handle dashboard data
            dashboard_metadata = data.get("dashboardMetadata", {})
            owner = dashboard_metadata.get("owner", {}).get("displayName", "")
            dashboard_attributes = data.get("attributes", {})
            
            dashboard_id = dashboard_metadata.get("id") or dashboard_attributes.get("dashboardId")
            dashboard_name = dashboard_metadata.get("name") or dashboard_attributes.get("dashboardName")
            dashboard_description = dashboard_metadata.get("description", "")
            
            properties_obj = {
                "dashboardId": dashboard_id,
                "dashboardName": dashboard_name,
                "description": dashboard_description,
                "owner": owner,
                "type": "dashboards"
            }
            
            log_info(f"Successfully updated dashboard data for {dashboard_name} (ID: {dashboard_id})")
        else:
            # Handle report data
            report_metadata = data.get("reportMetadata", {})
            report_attributes = data.get("attributes", {})
            
            report_id = report_metadata.get("id") or report_attributes.get("reportId")
            report_name = report_metadata.get("name") or report_attributes.get("reportName")
            report_description = report_metadata.get("description", "")
            report_type = report_metadata.get("reportType", {}).get("label", "")
            report_format = report_metadata.get("reportFormat", "")
            
            properties_obj = {
                "reportId": report_id,
                "reportName": report_name,
                "reportType": report_type,
                "detailColumns": report_metadata.get("detailColumns", []),
                "type": "reports"
            }
            
            log_info(f"Successfully updated report data for {report_name} (ID: {report_id})")
        
        connection = get_postgres_connection(config)
        with connection.cursor() as cursor:
            # Add statistics to properties_obj (Sigma pattern)
            if asset_type == "dashboards":
                # Dashboard statistics - get from database after views and columns are saved
                dashboard_components = data.get("dashboardMetadata", {}).get("components", [])
                dashboard_metadata = data.get("dashboardMetadata", {})
                description = __prepare_description(dashboard_metadata, asset_type)
                
                # Get column and table counts from database
                stats_query = f"""
                    select 
                        count(distinct reports_views.id) as tot_views,
                        count(distinct reports_columns.id) as tot_columns,
                        COUNT(distinct case when reports_columns.table_name IS NOT NULL AND reports_columns.table_name <> ''then reports_columns.table_name end) as tot_tables
                    from core.reports_views
                    left join core.reports_columns on reports_columns.report_view_id = reports_views.id
                    where reports_views.asset_id = '{asset_id}'
                """
                cursor = execute_query(connection, cursor, stats_query)
                stats = fetchone(cursor) or {}
                
                properties_obj.update({
                    "tot_reports": stats.get('tot_views', 0),
                    "tot_columns": stats.get('tot_columns', 0),
                    "tot_tables": stats.get('tot_tables', 0),
                    "tot_components": len(dashboard_components)
                })
            else:
                # Report statistics
                report_extended_metadata = data.get("reportExtendedMetadata", {})
                detail_columns_info = report_extended_metadata.get("detailColumnInfo", {})
                owner = None
                report_metadata = data.get("reportMetadata", {})
                description = __prepare_description(report_metadata, asset_type)
                
                # Get unique table names from columns
                table_names = set()
                for column_info in detail_columns_info.values():
                    fully_qualified_name = column_info.get("fullyQualifiedName", "")
                    if '.' in fully_qualified_name:
                        table_name = fully_qualified_name.split('.')[0]
                        table_names.add(table_name)
                
                properties_obj.update({
                    "tot_columns": len(detail_columns_info),
                    "tot_tables": len(table_names),
                    "tot_reports": 0  # Single report view
                })
            
            # Update core.reports with all properties including statistics
            properties_json = json.dumps(properties_obj, default=str).replace("'", "''")
            asset_properties.update({"owner": owner})
            asset_properties = json.dumps(asset_properties)
            
            query_string = f"""
                update core.reports set 
                    properties = '{properties_json}',
                    owner = '{owner}',
                    description = '{description}'
                where asset_id = '{asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            
            query_string = f"""
                update core.asset set properties = '{asset_properties}',
                description = '{description}'
                where id = '{asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
        
    except Exception as e:
        log_error("Salesforce Data Cloud Save Data Failed", e)
        raise e


def process_columns_for_view(
    cursor, connection, report_view_source_id, report_view_id, report_data, asset_id,
    connection_id, connection_type, component=None, config=None
):
    """
    Common function to process columns for both dashboard views and reports
    """
    try:
        # Get the database UUID for the report
        db_report_id = __get_report_table_id(config)
        
        report_extended_metadata = report_data.get("reportExtendedMetadata", {})
        detail_columns_info = report_extended_metadata.get("detailColumnInfo", {})

        insert_objects = []
        for column_key, column_info in detail_columns_info.items():
            fully_qualified_name = column_info.get("fullyQualifiedName", "")
            table_name = fully_qualified_name.split('.')[0] if '.' in fully_qualified_name else ""

            column_properties = {
                "column_key": column_key,
                "fullyQualifiedName": fully_qualified_name,
                "isLookup": column_info.get("isLookup", False),
                "filterable": column_info.get("filterable", False),
                "uniqueCountable": column_info.get("uniqueCountable", False),
                "filterValues": column_info.get("filterValues", []),
                "inactiveFilterValues": column_info.get("inactiveFilterValues", []),
                "entityColumnName": column_info.get("entityColumnName", ""),
                "column_type": "detail"
            }

            column_data = (
                str(uuid4()),
                column_info.get("label", ""),
                table_name,
                "",  # database
                "",  # schema
                column_info.get("dataType", ""),
                json.dumps(column_properties, default=str).replace("'", "''"),  # report_column_properties
                False,  # is_null
                False,  # is_semantics
                connection_type,
                report_view_source_id if report_view_source_id else None,  # source_id (Salesforce report ID like Sigma uses sheet_source_id)
                True,  # is_active
                False,  # is_delete
                report_view_id if report_view_id else None,  # report_view_id
                db_report_id,  # report_id (database UUID)
                asset_id,
                connection_id,
            )

            input_literals = ", ".join(["%s"] * len(column_data))
            query_param = cursor.mogrify(f"({input_literals})", column_data).decode("utf-8")
            insert_objects.append(query_param)

        if insert_objects:
            insert_objects = split_queries(insert_objects)
            for insert_object in insert_objects:
                query_input = ",".join(insert_object)
                query_string = f"""
                    insert into core.reports_columns(
                        id, name, table_name, database, schema, data_type, report_column_properties, is_null, is_semantics,
                        connection_type, source_id, is_active, is_delete, 
                        report_view_id, report_id, asset_id, connection_id
                    ) values {query_input}
                """
                cursor = execute_query(connection, cursor, query_string)

        if component and config:
            # Get the specific report view for this component
            component_id = component.get("id")
            

    except Exception as e:
        log_error(f"Error processing report {report_view_source_id}", e)

def save_report_columns(config: dict, data: dict):
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        connection_data = config.get("connection", {})
        credentials = connection_data.get("credentials")
        connection_type = connection_data.get("connection_type", "")
        credentials = decrypt_connection_config(credentials, connection_type)
        connection_type = config.get("connection_type", "")
        connection_id = connection_data.get("id")
        asset_type = config.get("type", "reports").lower()

        report_id = __get_report_table_id(config)
        connection = get_postgres_connection(config)

        with connection.cursor() as cursor:
            if asset_type == "dashboards":
                # Handle dashboard columns from components (Sigma pattern)
                dashboard_components = data.get("dashboardMetadata", {}).get("components", [])

                query_string = f""" delete from core.reports_columns 
                where asset_id = '{asset_id}'
                """
                cursor = execute_query(connection, cursor, query_string)
                
                # Then delete all existing views for this asset
                query_string = f""" delete from core.reports_views 
                    where asset_id = '{asset_id}'
                """
                cursor = execute_query(connection, cursor, query_string)

                for idx, component in enumerate(dashboard_components):
                    component_id = component.get("id")
                    component_header = component.get("header", "")
                    component_type = component.get("type", "Report")
                    report_id_in_component = component.get("reportId", "")
                    properties_obj = {
                    "componentId": component_id,
                    "componentHeader": component_header,
                    "componentType": component_type,
                    "reportId": report_id_in_component,
                    "source": "salesforce_data_cloud"
                    }
                    view_id = str(uuid4())
                    view_source_id = str(uuid4())
                
                    # Insert new view
                    query_input = (
                        view_id ,
                        view_source_id,
                        component_header,
                        component_type,
                        json.dumps(properties_obj, default=str).replace("'", "''"),
                        True,
                        False,
                        report_id,
                        asset_id,
                        connection_id,
                        datetime.now(),
                        datetime.now()
                    )
                    input_literals = ", ".join(["%s"] * len(query_input))
                    query_param = cursor.mogrify(f"({input_literals})", query_input).decode("utf-8")
                    
                    query_string = f"""
                        insert into core.reports_views(
                            id, source_id, name, type, properties, is_active, is_delete,
                            report_id, asset_id, connection_id, created_at, updated_at
                        ) values {query_param}
                    """
                    cursor = execute_query(connection, cursor, query_string)

                    if report_id_in_component:
                        try:
                            report_data = __get_report_detail(config, report_id_in_component, "reports", credentials)

                            if not report_data:
                                continue

                            # Clear existing columns ONLY for this specific report view

                            # Reuse common logic
                            process_columns_for_view(
                                cursor, connection, view_source_id, view_id, report_data,
                                asset_id, connection_id, connection_type, component, config
                            )
                            report_view = __get_report_view_table(config, report_id_in_component)
                            if report_view:
                                __update_view_statistics(config, report_view, component)

                        except Exception as e:
                            log_error(f"Error processing report {report_id_in_component}", e)
                            continue
            else:
                # Handle individual report columns - reuse same logic
                report_metadata = data.get("reportMetadata", {})
                source_report_id = report_metadata.get("id") or data.get("attributes", {}).get("reportId")
                report_view_id = None
                report_view_source_id = None # No view in report case

                # Clear existing columns for this asset
                query_string = f"""
                    delete from core.reports_columns
                    where asset_id = '{asset_id}' and report_id = '{report_id}'
                """
                cursor = execute_query(connection, cursor, query_string)

                # In report case, data already contains reportExtendedMetadata
                process_columns_for_view(
                    cursor, connection, report_view_source_id, report_view_id, data,
                    asset_id, connection_id, connection_type, None, config
                )

    except Exception as e:
        log_error("Salesforce Data Cloud Reports Save Columns Failed", e)
        raise e

def __get_report_table_id(config: dict) -> str:
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        connection = get_postgres_connection(config)

        with connection.cursor() as cursor:
            query_string = f"""
                select id from core.reports
                where asset_id='{asset_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            report = fetchone(cursor)
            report = report.get('id', None) if report else None
            return report
    except Exception as e:
        log_error(str(e), e)
        raise e

def __get_report_view_table(config: dict, report_id: str = None) -> dict:
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        connection = get_postgres_connection(config)

        with connection.cursor() as cursor:
            query_string = f"""
                select 
                    reports_views.id,
                    reports_views.source_id,
                    reports_views.properties,
                    count(distinct reports_columns.id) as tot_columns,
                    count(distinct case
                        when reports_columns.table_name is not null and reports_columns.table_name <> '' 
                        then reports_columns.table_name 
                    end) AS tot_tables
                from core.reports_views
                left join core.reports_columns on reports_columns.report_view_id = reports_views.id
                where reports_views.asset_id='{asset_id}' and reports_views.properties->>'reportId' = '{report_id}'
                group by reports_views.id
            """
            cursor = execute_query(connection, cursor, query_string)
            report = fetchone(cursor)
            return report
    except Exception as e:
        log_error(str(e), e)
        raise e

def __update_view_statistics(config: dict, report_view: dict, component: dict):
    """
    Update view statistics for dashboard components (Sigma pattern)
    """
    try:
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        connection = get_postgres_connection(config)
        
        with connection.cursor() as cursor:
            report_view_id = report_view.get("id")
            properties = report_view.get('properties', {})
            properties.update({
                "tot_columns": report_view.get('tot_columns', 0) if report_view.get('tot_columns') else 0,
                "tot_tables": report_view.get('tot_tables', 0) if report_view.get('tot_tables') else 0
            })
            
            query_string = f"""
                update core.reports_views set 
                    type = '{component.get('type', 'Report')}',
                    properties = '{json.dumps(properties, default=str).replace("'", "''")}'
                where id = '{report_view_id}'
            """
            cursor = execute_query(connection, cursor, query_string)
            
    except Exception as e:
        log_error(f"Salesforce Data Cloud - Update View Statistics Failed", e)
        raise e


def __prepare_reports_lineage(config, data):
    """
    Helper function to prepare Lineage for Salesforce Data Cloud Reports
    Similar to Sigma connector pattern
    """
    try:
        lineage = {
            "tables": [],
            "relations": []
        }

        lineage = __prepare_lineage_tables(config, lineage, data)
        lineage = __prepare_lineage_relations(config, lineage, data)

        # Save Lineage
        save_reports_lineage(config, {**lineage})

        # Save Propagation Values

    except Exception as e:
        log_error(f"Salesforce Data Cloud Reports - Prepare Lineage Failed ", e)
        raise e


def process_lineage_tables_for_report(config: dict, report_data: dict, component=None) -> list:
    """
    Common function to process lineage tables for both dashboard components and individual reports
    """
    try:
        report_metadata = report_data.get("reportMetadata", {})
        report_attributes = report_data.get("attributes", {})
        report_extended_metadata = report_data.get("reportExtendedMetadata", {})
        
        report_id = report_metadata.get("id") or report_attributes.get("reportId")
        report_name = report_metadata.get("name") or report_attributes.get("reportName")
        detail_columns_info = report_extended_metadata.get("detailColumnInfo", {})
        
        # Create report fields
        report_fields = []
        external_report_fields = []
        for column_key, column_info in detail_columns_info.items():
            field = {
                "id": column_key,
                "name": column_info.get("label", column_key),
                "type": "column",
                "data_type": column_info.get("dataType", ""),
                "fully_qualified_name": column_info.get("fullyQualifiedName", ""),
                "is_lookup": column_info.get("isLookup", False),
                "filterable": column_info.get("filterable", False),
                "unique_countable": column_info.get("uniqueCountable", False),
                "entity_column_name": column_info.get("entityColumnName", "")
            }
            report_fields.append(field)
            if field["fully_qualified_name"]:
                external_report_fields.append(field)
        
        # Create report table
        report_table = {
            "id": report_id,
            "name": report_name,
            "entity_name": report_id,
            "connection_type": "salesforce_data_cloud",
            "level": 3,
            "table_id": report_id,
            "table_name": report_name,
            "fields": report_fields
        }
        
        # Get unique table names from fully qualified names
        table_names = set()
        for column_key, column_info in detail_columns_info.items():
            fully_qualified_name = column_info.get("fullyQualifiedName", "")
            if '.' in fully_qualified_name:
                table_name = fully_qualified_name.split('.')[0]
                table_names.add(table_name)
        
        # Create external table objects
        external_tables = []
        for table_name in table_names:
            table_obj = {
                "id": table_name,
                "name": table_name,
                "entity_name": table_name,
                "connection_type": "salesforce_data_cloud",
                "level": 2,
                "table_id": table_name,
                "table_name": table_name,
                "fields": external_report_fields
            }
            external_tables.append(table_obj)
        
        # For dashboard components, modify the report table to use component info
        if component:
            component_id = component.get("id")
            component_header = component.get("header", "")
            report_table.update({
                "id": f"{component_id}_report",
                "name": component_header,
                "entity_name": component_id,
                "table_id": component_id,
                "table_name": component_header
            })
        
        # Combine all tables
        all_tables = external_tables + [report_table]
        return all_tables
        
    except Exception as e:
        log_error(f"Error processing lineage tables for report", e)
        return []

def __prepare_lineage_tables(config: dict, lineage: dict, data: dict) -> dict:
    """
    Helper function to prepare Lineage Tables for Salesforce Data Cloud Reports and Dashboards
    """
    try:
        asset_type = config.get("type", "reports").lower()
        
        if asset_type == "dashboards":
            # Dashboard lineage - process each component
            dashboard_metadata = data.get("dashboardMetadata", {})
            dashboard_id = dashboard_metadata.get("id")
            dashboard_name = dashboard_metadata.get("name")
            
            # Create dashboard table
            dashboard_table = {
                "id": dashboard_id,
                "name": dashboard_name,
                "entity_name": dashboard_id,
                "connection_type": "salesforce_data_cloud",
                "level": 3,
                "table_id": dashboard_id,
                "table_name": dashboard_name,
                "fields": []
            }
            
            all_tables = [dashboard_table]
            
            # Get credentials
            connection_data = config.get("connection", {})
            credentials = decrypt_connection_config(
                connection_data.get("credentials"), 
                connection_data.get("connection_type", "")
            )
            
            # Process each component using the common function
            for component in dashboard_metadata.get("components", []):
                component_id = component.get("id")
                report_id = component.get("reportId")
                report_view = __get_report_view_table(config, report_id)
                report_view_id = report_view.get("id")
                report_view_source_id = report_view.get("source_id")
                
                if not report_id:
                    continue
                    
                try:
                    # Fetch report data
                    report_data = __get_report_detail(config, report_id, "reports", credentials)
                    if not report_data:
                        continue
                    
                    # Use common function to process report lineage
                    component_tables = process_lineage_tables_for_report(config, report_data, component)
                    all_tables.extend(component_tables)
                            
                except Exception as e:
                    log_error(f"Error fetching report data for component {component_id}", e)
                    continue
            
            lineage["tables"] = all_tables
            return lineage
        else:
            # Handle individual report data using the common function
            component_tables = process_lineage_tables_for_report(config, data, None)
            lineage["tables"] = component_tables
            return lineage
        
    except Exception as e:
        log_error(f"Salesforce Data Cloud Reports - Prepare Lineage Tables Failed ", e)
        raise e


def process_lineage_relations_for_report(config: dict, report_data: dict, asset_id: str, report_view_source_id) -> list:
    """
    Common function to process lineage relations for both dashboard components and individual reports
    """
    try:
        report_extended_metadata = report_data.get("reportExtendedMetadata", {})
        detail_columns_info = report_extended_metadata.get("detailColumnInfo", {})
        
        relations = []
        
        # Create relations from external tables to report
        for column_key, column_info in detail_columns_info.items():
            fully_qualified_name = column_info.get("fullyQualifiedName", "")
            if '.' in fully_qualified_name:
                table_name = fully_qualified_name.split('.')[0]
                column_name = fully_qualified_name.split('.')[1]
                target_table_id = report_view_source_id if report_view_source_id else asset_id
                
                # For dashboard components, use component-specific target
                
                relation = {
                    "srcTableId": table_name,
                    "tgtTableId": target_table_id,
                    "srcTableColName": column_info.get("label", column_key),
                    "tgtTableColName": column_info.get("label", column_key)
                }
                relations.append(relation)
        
        return relations
        
    except Exception as e:
        log_error(f"Error processing lineage relations for report", e)
        return []

def __prepare_lineage_relations(config: dict, lineage: dict, data: dict) -> dict:
    """
    Helper function to prepare Lineage Relations for Salesforce Data Cloud Reports and Dashboards
    """
    try:
        asset_type = config.get("type", "reports").lower()
        asset = config.get("asset", {})
        asset_id = asset.get("id")
        
        if asset_type == "dashboards":
            # Dashboard relations - process each component
            dashboard_metadata = data.get("dashboardMetadata", {})
            
            # Get credentials
            connection_data = config.get("connection", {})
            credentials = decrypt_connection_config(
                connection_data.get("credentials"), 
                connection_data.get("connection_type", "")
            )
            
            # Process each component using the common function
            for component in dashboard_metadata.get("components", []):
                component_id = component.get("id")
                report_id = component.get("reportId")
                report_view = __get_report_view_table(config, report_id)
                report_view_id = report_view.get("id")
                report_view_source_id = report_view.get("source_id")
                
                if not report_id:
                    continue
                    
                try:
                    # Fetch report data
                    report_data = __get_report_detail(config, report_id, "reports", credentials)
                    if not report_data:
                        continue
                    
                    # Use common function to process report relations
                    component_relations = process_lineage_relations_for_report(config, report_data, asset_id, report_view_source_id)
                    lineage["relations"].extend(component_relations)
                    
                except Exception as e:
                    log_error(f"Error fetching report data for component {component_id}", e)
                    continue
        else:
            # Handle individual report data using the common function
            report_relations = process_lineage_relations_for_report(config, data, asset_id, None)
            lineage["relations"].extend(report_relations)
        
        return lineage
        
    except Exception as e:
        log_error(f"Salesforce Data Cloud - Prepare Lineage Relations Failed ", e)
        raise e

