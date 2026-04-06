import sys

# Fix 1: test_spark_submit_post_commands.py assertion
p_test = "C:/Users/prade/airflow/providers/apache/spark/tests/unit/apache/spark/hooks/test_spark_submit_post_commands.py"
with open(p_test, "r", encoding="utf-8") as f:
    test_text = f.read()

bad_assert = """        with patch("subprocess.run", return_value=mock_result) as mock_run:
            hook._run_post_submit_commands()
            mock_run.assert_called_once_with(
                "echo hello",
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                timeout=30,
            )"""

good_assert = """        with patch("subprocess.run", return_value=mock_result) as mock_run:
            hook._run_post_submit_commands()
            mock_run.assert_called_once_with(
                "echo hello",
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
                timeout=30,
            )"""

test_text = test_text.replace(bad_assert, good_assert)
with open(p_test, "w", encoding="utf-8") as f:
    f.write(test_text)

# Fix 2: spark_submit.py docstring
p_hook = "C:/Users/prade/airflow/providers/apache/spark/src/airflow/providers/apache/spark/hooks/spark_submit.py"
with open(p_hook, "r", encoding="utf-8") as f:
    hook_text = f.read()

bad_doc = """    def _run_post_submit_commands(self) -> None:
        \"\"\"
        Run any post-submit shell commands configured on this hook.
        Called after the Spark job finishes (success or on_kill). Typical use case"""

good_doc = """    def _run_post_submit_commands(self) -> None:
        \"\"\"
        Run any post-submit shell commands configured on this hook.

        Called after the Spark job finishes (success or on_kill). Typical use case"""

hook_text = hook_text.replace(bad_doc, good_doc)
with open(p_hook, "w", encoding="utf-8") as f:
    f.write(hook_text)

print("done applying python fixes")
