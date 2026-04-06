with open(r"C:\Users\prade\airflow\airflow-core\docs\migrations-ref.rst", "r", encoding="utf-8") as f:
    lines = f.readlines()

new_rows = []
new_rows.append("+-------------------------+------------------+-------------------+--------------------------------------------------------------+\n")
new_rows.append("| ``35ab6b577738`` (head) | ``1d6611b6ab7c`` | ``3.2.1``         | Set bundle_name to 'dags-folder' for legacy DAGs with NULL   |\n")
new_rows.append("|                         |                  |                   | bundle_name and make column non-nullable.                    |\n")

for i in range(len(lines)):
    line = lines[i]
    if "``1d6611b6ab7c`` (head)" in line:
        lines[i] = line.replace("``1d6611b6ab7c`` (head)", "``1d6611b6ab7c``        ")
        
    if "+=========================+==================+===================+==============================================================+" in line:
        lines.insert(i+1, "".join(new_rows))
        break

with open(r"C:\Users\prade\airflow\airflow-core\docs\migrations-ref.rst", "w", encoding="utf-8", newline="\n") as f:
    f.writelines(lines)
    
print("Updated migrations-ref.rst successfully!")
