import os
import subprocess
from urllib.parse import urlparse

from flask import Flask, render_template_string, request

app = Flask(__name__)

INDEX_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Neon DB Migration</title>
    <style>
        body { font-family: sans-serif; margin: 2em; background-color: #f4f4f4; color: #333; }
        .container { max-width: 800px; margin: auto; background: white; padding: 2em; border-radius: 8px; box-shadow: 0 0 10px rgba(0,0,0,0.1); }
        h1 { color: #007bff; }
        textarea { width: 100%; box-sizing: border-box; padding: 10px; margin-bottom: 1em; border-radius: 4px; border: 1px solid #ddd; }
        input[type="submit"] { background: #007bff; color: white; padding: 10px 15px; border: none; border-radius: 4px; cursor: pointer; font-size: 16px; }
        input[type="submit"]:hover { background: #0056b3; }
        pre { background: #eee; padding: 1em; border-radius: 4px; white-space: pre-wrap; word-wrap: break-word; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Neon PostgreSQL Database Migration</h1>
        <form method="post">
            <label for="source">Source Connection String:</label><br>
            <textarea id="source" name="source" rows="3" required></textarea><br>
            <label for="target">Target Connection String:</label><br>
            <textarea id="target" name="target" rows="3" required></textarea><br>
            <input type="submit" value="Migrate Database">
        </form>
        {% if output %}
        <h2>Output:</h2>
        <pre>{{ output }}</pre>
        {% endif %}
    </div>
</body>
</html>
"""


def parse_db_string(db_string):
    result = urlparse(db_string)
    return {
        "username": result.username,
        "password": result.password,
        "hostname": result.hostname,
        "dbname": result.path[1:],
    }


@app.route("/", methods=["GET", "POST"])
def index():
    output = None
    if request.method == "POST":
        source_conn_str = request.form["source"]
        target_conn_str = request.form["target"]

        source_db = parse_db_string(source_conn_str)
        target_db = parse_db_string(target_conn_str)

        backup_file = "neon_backup.dump"
        output_log = []

        try:
            dump_env = os.environ.copy()
            if source_db.get("password"):
                dump_env["PGPASSWORD"] = source_db["password"]

            dump_command = [
                "pg_dump",
                "-h",
                source_db["hostname"],
                "-U",
                source_db["username"],
                "-d",
                source_db["dbname"],
                "-F",
                "c",
                "-f",
                backup_file,
                "--no-owner",
            ]
            output_log.append(f"Running pg_dump...\n$ {' '.join(dump_command)}\n")

            dump_result = subprocess.run(
                dump_command, env=dump_env, capture_output=True, text=True, check=False
            )
            output_log.append(dump_result.stdout)
            output_log.append(dump_result.stderr)

            if dump_result.returncode != 0:
                output_log.append("\npg_dump failed. Aborting migration.")
                raise Exception("pg_dump failed")

            output_log.append("\npg_dump completed successfully.\n")

            output_log.append(
                "\nEnsuring 'public' schema exists in target database...\n"
            )
            psql_env = os.environ.copy()
            if target_db.get("password"):
                psql_env["PGPASSWORD"] = target_db["password"]

            create_schema_command = [
                "psql",
                "-h",
                target_db["hostname"],
                "-U",
                target_db["username"],
                "-d",
                target_db["dbname"],
                "-c",
                "CREATE SCHEMA IF NOT EXISTS public;",
            ]
            output_log.append(f"$ {' '.join(create_schema_command)}\n")

            create_schema_result = subprocess.run(
                create_schema_command,
                env=psql_env,
                capture_output=True,
                text=True,
                check=False,
            )

            output_log.append(create_schema_result.stdout)
            output_log.append(create_schema_result.stderr)

            if create_schema_result.returncode != 0:
                output_log.append(
                    "\nWarning: Command to create schema failed. This may be okay if it already exists and permissions are restricted.\n"
                )
            else:
                output_log.append("Schema 'public' check/creation complete.\n")

            restore_env = os.environ.copy()
            if target_db.get("password"):
                restore_env["PGPASSWORD"] = target_db["password"]

            restore_command = [
                "pg_restore",
                "-h",
                target_db["hostname"],
                "-U",
                target_db["username"],
                "-d",
                target_db["dbname"],
                "--clean",
                "--if-exists",
                "--no-owner",
                "--no-privileges",
                "--no-comments",
                "-F",
                "c",
                backup_file,
            ]
            output_log.append(
                f"\nRunning pg_restore...\n$ {' '.join(restore_command)}\n"
            )

            restore_result = subprocess.run(
                restore_command,
                env=restore_env,
                capture_output=True,
                text=True,
                check=False,
            )
            output_log.append(restore_result.stdout)
            output_log.append(restore_result.stderr)

            if restore_result.returncode != 0:
                output_log.append(
                    "\npg_restore finished with errors (this is often acceptable for Neon migrations)."
                )
            else:
                output_log.append("\npg_restore completed successfully.")

        except Exception as e:
            if "pg_dump failed" not in str(e):
                output_log.append(f"\nAn unhandled error occurred: {e}")
        finally:
            if os.path.exists(backup_file):
                os.remove(backup_file)

        output = "\n".join(output_log)

    return render_template_string(INDEX_HTML, output=output)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
