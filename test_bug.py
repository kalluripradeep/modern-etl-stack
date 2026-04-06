from airflow.api_fastapi.app import create_app
import os
os.environ["AIRFLOW__CORE__AUTH_MANAGER"] = "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager"
os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = "sqlite:///:memory:"

try:
    app = create_app()
    print("SUCCESS")
except Exception as e:
    import traceback
    traceback.print_exc()
