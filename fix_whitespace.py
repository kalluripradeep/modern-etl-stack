import sys
p = "C:/Users/prade/airflow/airflow-core/src/airflow/migrations/versions/0110_3_2_0_set_bundle_name_non_nullable.py"
try:
    with open(p, "r", encoding="utf-8") as f:
        lines = f.readlines()
    changed = False
    new_lines = [line.rstrip() + "\n" for line in lines]
    if new_lines != lines: changed = True
    while len(new_lines) > 0 and new_lines[-1].strip() == "":
        new_lines.pop()
        changed = True
    if len(new_lines) == 0 or not new_lines[-1].endswith("\n"):
        new_lines.append("\n")
        changed = True
    if changed:
        with open(p, "w", encoding="utf-8") as f:
            f.writelines(new_lines)
        print("Fixed EOF and trailing ws")
    else:
        print("No whitespace issues")
except FileNotFoundError:
    print("File not found")
