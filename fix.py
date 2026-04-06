import sys
p = "C:/Users/prade/airflow/providers/apache/spark/src/airflow/providers/apache/spark/hooks/spark_submit.py"
with open(p, "r", encoding="utf-8") as f:
    text = f.read()
bad = """        try:
            func = getattr(kerberos, "get_kerberos_principle")
        except AttributeError:
            # Fallback for older versions of Airflow
            func = getattr(kerberos, "get_kerberos_principle")"""
good = """        try:
            func = kerberos.get_kerberos_principal
        except AttributeError:
            # Fallback for older versions of Airflow
            func = getattr(kerberos, "get_kerberos_principle")"""
text = text.replace(bad, good)
with open(p, "w", encoding="utf-8") as f:
    f.write(text)
print("done")
