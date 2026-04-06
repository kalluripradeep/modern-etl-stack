import sys

p = "C:/Users/prade/airflow/airflow-core/src/airflow/migrations/versions/0108_3_2_0_set_bundle_name_non_nullable.py"
with open(p, "r", encoding="utf-8") as f:
    text = f.read()

bad = """def upgrade():
    \"\"\"Set bundle_name to 'dags-folder' for legacy DAGs with NULL bundle_name and make column non-nullable.\"\"\"
    op.execute("UPDATE dag SET bundle_name = 'dags-folder' WHERE bundle_name IS NULL")
    with op.batch_alter_table("dag", schema=None) as batch_op:"""

good = """def upgrade():
    \"\"\"Set bundle_name to 'dags-folder' for legacy DAGs with NULL bundle_name and make column non-nullable.\"\"\"
    dag = sa.table(
        "dag",
        sa.column("bundle_name", sa.String(length=200)),
    )
    op.execute(
        dag.update()
        .where(dag.c.bundle_name.is_(None))
        .values(bundle_name="dags-folder")
    )
    with op.batch_alter_table("dag") as batch_op:"""

text = text.replace(bad, good)
with open(p, "w", encoding="utf-8") as f:
    f.write(text)
print("done")
