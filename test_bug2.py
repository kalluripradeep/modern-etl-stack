from airflow.api_fastapi.app import create_app
import os
os.environ["AIRFLOW__CORE__TEST_MODE"] = "True"
os.environ["AIRFLOW__CORE__AUTH_MANAGER"] = "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager"

try:
    from airflow.utils import db
    db.initdb()
    app = create_app()
    print("SUCCESS")
except Exception as e:
    import traceback
    traceback.print_exc()
