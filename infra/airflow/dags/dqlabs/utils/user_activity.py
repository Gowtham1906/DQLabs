import json
from uuid import uuid4
import uuid
from dqlabs.app_helper.dag_helper import get_postgres_connection
from dqlabs.app_helper.db_helper import execute_query, fetchone, fetchall
from dqlabs.enums.connection_types import ConnectionType

def is_valid_uuid(value):
    try:
        uuid.UUID(value)
        return True
    except ValueError:
        return False

def getsubtype(subType, measureCategory, value):
    if (subType):
        propertyName = ""
        if subType == "is_active":
            propertyName = "Active Flag"
        elif subType == "is_auto":
            propertyName = "Manual Threshold"
        elif subType == "allow_score":
            propertyName = "Scoring"
        elif subType == "is_drift_enabled":
            propertyName = "Monitoring"
        elif subType == "is_positive":
            propertyName = "Polarity"
        elif subType == "status":
            propertyName = "Status"
        elif subType == "properties":
            propertyName = "Query"
            if ((measureCategory == "behavioral" or measureCategory == "comparison") and isinstance(value, dict)):
                value['current_value'] = json.dumps( value.get('current_value') if value.get('current_value') else "")
                value['prev_value'] = json.dumps(value.get('prev_value') if value.get('prev_value') else "")
        elif subType == "override_limit_config":
            propertyName = "Override Limit Config"
            if (isinstance(value, dict)):
                value['current_value'] = json.dumps( value.get('current_value') if value.get('current_value') else "")
                value['prev_value'] = json.dumps(value.get('prev_value') if value.get('prev_value') else "")
        elif subType == "override_limit_row_count":
            propertyName = "Override Row Limit"
            if (isinstance(value, dict)):
                value['current_value'] = json.dumps( value.get('current_value') if value.get('current_value') else "")
                value['prev_value'] = json.dumps(value.get('prev_value') if value.get('prev_value') else "")
        elif subType == "override_limit_column_count":
            propertyName = "Override Column Limit"
        elif subType == "failed_rows_query_config" or subType == "total_records_query_config":
            propertyName = "Failed Rows Query Config" if subType == "failed_rows_query_config" else "Total Records Query Config"
        elif subType == "failed_rows_query" or subType == "total_records_query":
            propertyName = "Failed Rows Query" if subType == "failed_rows_query" else "Total Records Query"
            if (isinstance(value, dict)):
                value['current_value'] = json.dumps( value.get('current_value') if value.get('current_value') else "")
                value['prev_value'] = json.dumps(value.get('prev_value') if value.get('prev_value') else "")
        elif subType == "threshold_constraints":
            propertyName = "Manual Threshold Constrain"
        elif subType == "drift_threshold":
            propertyName = "Threshold values"
            if (isinstance(value, dict)):
                value['current_value'] = json.dumps( value.get('current_value') if value.get('current_value') else "")
                value['current_value'] = value['current_value'].replace("<attribute>", "<value>")
                value['prev_value'] = json.dumps(value.get('prev_value') if value.get('prev_value') else "")
                value['prev_value'] = value['prev_value'].replace("<attribute>", "<value>")
                if (len(value['prev_value']) <= 0):
                    value['prev_value'] = "auto threshold"
        elif subType == "domains":
            return f"""measure domain have been updated by"""
        elif subType == "application":
            return f"""measure application have been updated by"""
        elif subType == "description":
            return f"""measure description have been updated by"""
        else:
            propertyName = ""
        if (
            subType == "override_limit_config"
            or subType == "failed_rows_query_config"
            or subType == "total_records_query_config"
        ):
            return f"""measure property {propertyName} have been updated by"""
        if (propertyName):
            return f"""measure property {propertyName} updated to {json.dumps(value['current_value'])} from {json.dumps(value['prev_value'])} by"""
    return f"""measure properties have been updated by"""

def getVersionText(data):
    try:
        auditTypeText = ""
        measureCategory = data.get('measure_category') if data.get('measure_category') else "";
        value = data.get('value')
        if (data.get('measure_name') or data.get('conversation_name') or data.get('term_name') or data.get('usage_name') or data.get('value')):
            if data.get('measure_name'):
                auditTypeText = data.get('measure_name')
                
            if data.get('conversation_name'):
                auditTypeText = data.get('conversation_name')
                
            if data.get('term_name'):
                auditTypeText = data.get('term_name')
                
            if data.get('usage_name'):
                auditTypeText = data.get('usage_name')
                
            if data.get('value'):
                auditTypeText = data.get('value')
                
        primaryText = "has been updated";
        
        primaryText = primaryText if len(auditTypeText)>0 else f"""{auditTypeText} {primaryText}""";
        
        if data.get('type') == "create_asset":
            return 'Asset created by'
        if data.get('type') == "update_description":
            return f"""Description {primaryText}"""
        if data.get('type') == "update_domain":
            return f"""Domain {primaryText}"""
        if data.get('type') == "update_application":
            return f"""Application {primaryText}"""
        if data.get('type') == "update_identifier":
            return f"""Identifier {primaryText}"""
        if data.get('type') == "run_now":
            return f"""Trigger a job by"""
        if data.get('type') == "update_status":
            return f"""Status {primaryText}"""
        if data.get('type') == "update_null":
            return f"""Null {primaryText}"""
        if data.get('type') == "update_max_length":
            return f"""Max Length {primaryText}"""
        if data.get('type') == "update_min_length":
            return f"""Min Length {primaryText}"""
        if data.get('type') == "update_min_value":
            return f"""Min Value {primaryText}"""
        if data.get('type') == "update_max_value":
            return f"""Max Value {primaryText}"""
        if data.get('type') == "update_unique":
            return f"""Unique {primaryText}"""
        if data.get('type') == "update_blank":
            return f"""Blank {primaryText}"""
        if data.get('type') == "update_primary":
            return f"""Primary {primaryText}"""
        if data.get('type') == "update_tags":
            return f"""Tags {primaryText}"""
        if data.get('type') == "create_schedule":
            return f"""Asset Schedule added by"""
        if data.get('type') == "edit_schedule":
            return f"""Asset Schedule {primaryText}"""
        if data.get('type') == "delete_schedule":
            return f"""Asset Schedule has been delete by"""
        if data.get('type') == "update_pattern":
            return f"""Pattern {primaryText}"""
        if data.get('type') == "update_enum":
            return f"""Enum {primaryText}"""
        if data.get('type') == "create_measure":
            value = json.loads(value) if isinstance(value, str) else value
            value = value if value else {}
            query = value.get('query') if value.get('query', '') else ''
            return f"""measure has been created with query {query} by"""
        if data.get('type') == "edit_measure":
            subType = data.get('sub_type', '')
            value = json.loads(value) if isinstance(value, str) else value
            value = value if value else {}
            if (subType):
                return getsubtype(subType, measureCategory, value)
            return f"""measure properties have been updated by""";
        if data.get('type') == "delete_measure":
            return f"""measure deleted by""";
        if data.get('type') == "update_steward_user":
            return f"""Steward User {primaryText}""";
        if data.get('type') == "create_conversation":
            return f"""conversation created by"""
        if data.get('type') == "update_conversation":
            return f"""conversation updated by"""
        if data.get('type') == "delete_conversation":
            return f"""conversation deleted by"""
        if data.get('type') == "reply_conversation":
            return f"""conversation on comment replied by"""
        if data.get('type') == "update_reply_conversation":
            return f"""conversation on comment updated by"""
        if data.get('type') == "delete_reply_conversation":
            return f"""conversation on comment deleted by"""
        if data.get('type') == "term_approve":
            return 'term has been approved by'
        if data.get('type') == "term_reject":
            return 'term has been rejected by'
        if data.get('type') == "term_delete":
            return 'term has been removed by'
        if data.get('type') == "update_term":
            return f"""term {primaryText}"""
        if data.get('type') == "create_usage":
            return f"""usage created by"""
        if data.get('type') == "update_usage":
            return f"""usage updated by"""
        if data.get('type') == "delete_usage":
            return f"""usage deleted by"""
        if data.get('type') == "primary_columns":
            return f"""primary_columns {primaryText}"""
        if data.get('type') == "run_semantic_discovery":
            return 'Trigger a Semantic Discovery by'
        if data.get('type') == "import_metadata":
            return f"""File has been imported metadata by"""
        if data.get('type') == "import_measure":
            return f"""File has been imported measure by"""
        return ""
    except Exception as e:
        return ""

def get_user_activity_log(config: dict, search_data: dict):
    connection = get_postgres_connection(config)
    exits = False
    schedule_count = []
    output_details = None
    with connection.cursor() as cursor:
        schedule_select_query = f"""
            SELECT connection_id, start_time, created_date FROM core.independent_schedule_logs 
            where schema='{search_data.get('schema', '')}' and 
            database='{search_data.get('database', '')}' and 
            connection_id='{search_data.get('connection_id')}' and start_time is not null and
            status='Completed'
            order by created_date desc limit 1
        """
        cursor = execute_query(connection, cursor, schedule_select_query)
        schedule_count = fetchall(cursor)
        if schedule_count:
            exits = True
            output_details = schedule_count[0]
    return exits, output_details

def insert_user_activity_log(config: dict, insert_data: dict, is_fullmetadata: bool = False):
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:

        # Check if a record with the specified queue_id exists
        check_queue_query = f"SELECT id FROM core.independent_schedule_logs WHERE queue_id = '{insert_data.get('queue_id')}'"
        cursor = execute_query(connection, cursor, check_queue_query)
        existing_record = fetchone(cursor)

        if existing_record:
            # If record exists, update it
            schedule_query = f"""
                UPDATE core.independent_schedule_logs
                SET connection_id = '{insert_data.get('connection_id')}',
                    status = '{insert_data.get('status')}',
                    created_date = CURRENT_TIMESTAMP,
                    database = '{insert_data.get('database')}',
                    schema = '{insert_data.get('schema')}'
                WHERE id = '{existing_record.get("id")}'
                RETURNING id
            """
        else:
            # If record doesn't exist, insert a new one
            schedule_query = f"""
                insert into core.independent_schedule_logs (id, connection_id, queue_id, status, created_date, database, schema)
                values ('{str(uuid4())}', '{insert_data.get('connection_id')}', '{insert_data.get('queue_id')}', '{insert_data.get('status')}', CURRENT_TIMESTAMP, '{insert_data.get('database')}', '{insert_data.get('schema')}')
                RETURNING id
            """
        cursor = execute_query(
            connection, cursor, schedule_query)
            
        insert_details = fetchall(cursor)
        insert_details = insert_details[0] if insert_details else []
        return insert_details

def save_user_activity_log(config: dict, insert_data: dict):
    connection = get_postgres_connection(config)
    with connection.cursor() as cursor:
        if insert_data.get('start_time'):
            schedule_insert_query = f"""
                UPDATE core.independent_schedule_logs
                SET status = '{insert_data.get('status')}', start_time = '{insert_data.get('start_time')}'
                WHERE id='{insert_data.get('id')}'
            """
        else:
            schedule_insert_query = f"""
                UPDATE core.independent_schedule_logs
                SET status = '{insert_data.get('status')}'
                WHERE id='{insert_data.get('id')}'
            """
        cursor = execute_query(
            connection, cursor, schedule_insert_query)

def get_user_activity(config: dict, last_push_date):
    connection = get_postgres_connection(config)
    organization_id = config.get("dag_info").get("organization_id")
    destination_connection_object = config.get("destination_connection_object")
    filter_query = f"""where al.created_date > '{last_push_date}'""" if last_push_date else f""""""
    with connection.cursor() as cursor:
        query_string = f"""
            SELECT al.created_by as USER_ID,
            CONCAT(users.first_name, ' ', users.last_name) AS USERNAME,
            al.role as USERROLE, al.email as Email, al.attribute_id, al.asset_id,
            al.created_date as CREATED_DATE, 
            al.page, al.module, al.submodule, al.message,
            ms.technical_name as measure_name
            FROM core.audit_logs as al
            left join core.measure as ms on cast(ms.id as TEXT) = cast(al.measure_id as TEXT)
            join core.users as users on cast(users.id as TEXT)=cast(al.created_by as TEXT)
            {filter_query}
            order by al.created_date
        """
        cursor = execute_query(connection, cursor, query_string)
        user_activity = fetchall(cursor)
        activity = []
        asset_details={}
        attribute_details={}
        connection_details={}
        for useractivity in user_activity:
            dict_activity = {}
            dict_activity['USER_ID'] = useractivity.get('user_id')
            dict_activity['USERNAME'] = useractivity.get('username')
            dict_activity['USERROLE'] = useractivity.get('userrole')
            dict_activity['ATTRIBUTE_ID'] = useractivity.get('attribute_id')
            dict_activity['CREATED_DATE'] = useractivity.get('created_date')
            dict_activity['PAGE'] = useractivity.get('page')
            dict_activity['MODULE'] = useractivity.get('module')
            dict_activity['SUBMODULE'] = useractivity.get('submodule')
            dict_activity['ASSETS'] = ""
            dict_activity['CONNECTION_ID'] = ""
            dict_activity['ATTRIBUTE'] = ""
            version_text = useractivity.get('message')
            version_text = str(version_text).replace('"', "").replace("'", "")
            username = useractivity.get('username')
            destination_connection_type = destination_connection_object.get("type").lower()
            
            try:
                # Truncate version_text if it exceeds 2000 characters
                version_text_limit = 0
                if destination_connection_type == ConnectionType.Oracle.value:
                    version_text_limit = 3990
                elif destination_connection_type == ConnectionType.Synapse.value:
                    version_text_limit = 7990
                if version_text_limit > 0:
                    if len(str(version_text)) + len(str(username)) > version_text_limit:
                        max_version_text_length = version_text_limit - len(username)
                        version_text = version_text[:max_version_text_length]
            except Exception as e:
                version_text = ""

            # Concatenate version_text and username
            version_text = f"""{version_text} {username}""" if version_text else ""

            # Assign version_text to NOTIFICATION_TEXT
            dict_activity['NOTIFICATION_TEXT'] = version_text
            
            if useractivity.get('asset_id') and is_valid_uuid(useractivity.get('asset_id')):
                if useractivity.get('asset_id') in asset_details.keys():
                    dict_activity['ASSETS'] = asset_details[useractivity.get('asset_id')]
                    dict_activity['CONNECTION_ID'] = connection_details[useractivity.get('asset_id')]
                else:
                    assets_query = f"""
                        select technical_name, connection_id from core.asset where id='{useractivity.get('asset_id')}'
                    """
                    cursor = execute_query(connection, cursor, assets_query)
                    asset_details_value = fetchone(cursor)
                    dict_activity['ASSETS'] = asset_details_value.get('technical_name').replace("'","") if asset_details_value else ''
                    dict_activity['CONNECTION_ID'] = asset_details_value.get('connection_id') if asset_details_value else ''
                    if asset_details_value:
                        asset_details[useractivity.get('asset_id')] = asset_details_value.get('technical_name').replace("'","") 
                        connection_details[useractivity.get('asset_id')] = asset_details_value.get('connection_id')
                        
            if useractivity.get('attribute_id') and is_valid_uuid(useractivity.get('attribute_id')):
                if useractivity.get('attribute_id') in attribute_details.keys():
                    dict_activity['ATTRIBUTE'] = attribute_details[useractivity.get('attribute_id')]
                else:
                    assets_query = f"""
                        select technical_name from core.attribute where id='{useractivity.get('attribute_id')}'
                    """
                    cursor = execute_query(connection, cursor, assets_query)
                    attribute_details_value = fetchone(cursor)
                    dict_activity['ATTRIBUTE'] = attribute_details_value.get('technical_name').replace("'","") if attribute_details_value else ''
                    if attribute_details_value:
                        attribute_details[useractivity.get('attribute_id')] = attribute_details_value.get('technical_name')
                    
            activity.append(dict_activity)
        return activity

def get_user_session(config: dict):
    connection = get_postgres_connection(config)
    organization_id = config.get("dag_info").get("organization_id")
    with connection.cursor() as cursor:
        query_string = f"""
             select users.id as USER_ID, users.last_login, roles.name as USERROLE, 
            CONCAT(users.first_name, ' ', users.last_name) AS USERNAME, users.email as USERMAILID
            from core.users as users
            inner join core.roles as roles on roles.id=users.role_id
            where users.organization_id='{organization_id}'
        """
        query_details_cursor = execute_query(connection, cursor, query_string)
        user_sessions = fetchall(query_details_cursor)
        usersessions = []
        for user_session in user_sessions:
            user_id=user_session.get('USER_ID')
            if not user_id:
                user_id=user_session.get('user_id')
            query_string_session= f"""
                WITH user_activity_session
                    AS (
                        SELECT jsonb_array_elements_text(coalesce(json_agg(DISTINCT (EXTRACT(EPOCH FROM (user_session_track.session_end_time - user_session_track.session_start_time)))))::jsonb) AS session_time
                        FROM core.user_session_track
                        WHERE organization_id = '{organization_id}'
                            AND user_id = '{user_id}'
                        )
                    SELECT min(session_time::FLOAT) AS minimum_duration
                        ,max(session_time::FLOAT) AS maximum_duration
                        ,avg(session_time::FLOAT) AS average_duration
                    FROM user_activity_session
            """
            user_login_cursor = execute_query(connection, cursor, query_string_session)
            user_login_details = fetchall(user_login_cursor)
            
            minimum_duration = None
            maximum_duration = None
            average_duration = 0

            for session in user_login_details:
                minimum_duration = session.get("minimum_duration")
                maximum_duration = session.get("maximum_duration")
                average_duration = session.get("average_duration")

            # activity log count
            total_login_count = 0
            activity_log_query = f"""
                SELECT 
                    COUNT(DISTINCT id) AS login_count
                FROM core.user_session_track
                WHERE user_id = '{user_id}'
            """
            activity_cursor = execute_query(connection, cursor, activity_log_query)
            activity_log_details = fetchall(activity_cursor)
            for activity_log_detail in activity_log_details:
                total_login_count = activity_log_detail.get("login_count",0)
                total_login_count = int(total_login_count) if total_login_count else 0

            # audit log count 
            audit_log_query = f"""
                SELECT vh.created_by,
                    COUNT(DISTINCT vh.id) AS no_of_logs
                FROM core.version_history vh
                WHERE vh.created_by = '{user_id}'
                GROUP BY vh.created_by
            """
            cursor = execute_query(connection, cursor, audit_log_query)
            audit_log_details = fetchall(cursor)
            audit_log_details_dict = {}
            audit_log_count = 0
            for audit_log_detail in audit_log_details:
                audit_log_details_dict.update({
                    f"""{audit_log_detail.get('created_by')}""": audit_log_detail.get('no_of_logs')
                })
                audit_log_count = audit_log_detail.get("no_of_logs",0)
                audit_log_count = int(audit_log_count) if audit_log_count else 0

            dict_user_session = {
                "USER_ID": user_session.get('user_id'),
                "LAST_LOGGED_IN": user_session.get('last_login'),
                "USERROLE": user_session.get('userrole'),
                "USERNAME": user_session.get('username'),
                "USERMAILID": user_session.get('usermailid'),
                "TOTAL_LOGIN_COUNT": total_login_count,
                "AVG_SESSION_TIME": average_duration,
                "MIN_SESSION_TIME": minimum_duration if minimum_duration else 0,
                "MAX_SESSION_TIME": maximum_duration if maximum_duration else 0,
                "AUDITS_COUNT": audit_log_count
            }
            
            usersessions.append(dict_user_session)
            
        return usersessions
