# dqlabs_agent stub — local dev only (connector wheel not installed)
# Provides no-op stubs so DAGs can import without errors.


class DQAgent:
    @staticmethod
    def is_enabled(connection_type: str) -> bool:
        return False

    @staticmethod
    def is_vault_enabled(vault_type: str) -> bool:
        return False

    def execute(self, **kwargs):
        return {"status": "success", "data": None}

    def clear_connection(self, task_id):
        pass

    def get_vault_secret(self, vault_config: dict) -> dict:
        return {}
