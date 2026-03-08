# dqlabs_agent stub — local dev only


class DQAgentService:
    def __init__(self, agent_url: str, client_id: str, client_secret: str):
        self.agent_url = agent_url
        self.client_id = client_id
        self.client_secret = client_secret

    def execute(self, input_data: dict):
        return {"status": "success", "data": None}

    def clear_connection(self, task_id):
        pass

    def get_vault_data(self, vault_config: dict) -> dict:
        return {}
