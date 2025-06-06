import os
import subprocess
from urllib.parse import urlparse

from flask import Flask, Response, render_template_string, request

app = Flask(__name__)

INDEX_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Neon DB Migration</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;700&display=swap" rel="stylesheet">
    <style>
        :root {
            --primary-color: #007bff;
            --primary-hover: #0056b3;
            --background-color: #f8f9fa;
            --container-bg: #ffffff;
            --text-color: #343a40;
            --border-color: #dee2e6;
            --output-bg: #f1f3f5;
        }
        body {
            font-family: 'Inter', sans-serif;
            margin: 0;
            background-color: var(--background-color);
            color: var(--text-color);
            display: flex;
            justify-content: center;
            align-items: center;
            min-height: 100vh;
            padding: 2em;
        }
        .container {
            width: 100%;
            max-width: 800px;
            background: var(--container-bg);
            padding: 2.5em;
            border-radius: 12px;
            box-shadow: 0 8px 30px rgba(0,0,0,0.08);
            text-align: center;
        }
        h1 {
            color: var(--primary-color);
            margin-bottom: 1.5em;
        }
        form {
            display: flex;
            flex-direction: column;
            gap: 1.5em;
        }
        textarea {
            width: 100%;
            box-sizing: border-box;
            padding: 12px;
            border-radius: 6px;
            border: 1px solid var(--border-color);
            font-family: monospace;
            font-size: 14px;
            resize: vertical;
        }
        input[type="submit"] {
            background: var(--primary-color);
            color: white;
            padding: 12px 20px;
            border: none;
            border-radius: 6px;
            cursor: pointer;
            font-size: 16px;
            font-weight: 500;
            transition: background-color 0.2s;
        }
        input[type="submit"]:hover:not(:disabled) {
            background: var(--primary-hover);
        }
        input[type="submit"]:disabled {
            background-color: #a0c3ff;
            cursor: not-allowed;
        }
        #output-container {
            margin-top: 2em;
            text-align: left;
            display: none;
        }
        pre {
            background: var(--output-bg);
            padding: 1.5em;
            border-radius: 6px;
            white-space: pre-wrap;
            word-wrap: break-word;
            max-height: 400px;
            overflow-y: auto;
            font-family: monospace;
            font-size: 14px;
            line-height: 1.5;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Neon PostgreSQL Database Migration</h1>
        <form>
            <div>
                <label for="source" style="display: block; text-align: left; margin-bottom: 0.5em;">Source Connection String:</label>
                <textarea id="source" name="source" rows="3" required></textarea>
            </div>
            <div>
                <label for="target" style="display: block; text-align: left; margin-bottom: 0.5em;">Target Connection String:</label>
                <textarea id="target" name="target" rows="3" required></textarea>
            </div>
            <input type="submit" value="Migrate Database">
        </form>
        <div id="output-container">
            <h2>Output:</h2>
            <pre id="output"></pre>
        </div>
    </div>
    <script>
        const form = document.querySelector('form');
        const outputEl = document.getElementById('output');
        const outputContainer = document.getElementById('output-container');
        const submitButton = form.querySelector('input[type="submit"]');

        form.addEventListener('submit', async (e) => {
            e.preventDefault();
            outputEl.innerHTML = '';
            outputContainer.style.display = 'block';
            submitButton.disabled = true;
            submitButton.value = 'Migrating...';

            const formData = new FormData(form);

            try {
                const response = await fetch('/migrate', {
                    method: 'POST',
                    body: formData
                });

                const reader = response.body.getReader();
                const decoder = new TextDecoder();

                while (true) {
                    const { done, value } = await reader.read();
                    if (done) break;
                    
                    const chunk = decoder.decode(value, { stream: true });
                    outputEl.innerHTML += chunk;
                    outputEl.scrollTop = outputEl.scrollHeight;
                }
            } catch (error) {
                outputEl.innerHTML += `\\n--- Frontend Error ---\\nAn error occurred: ${error}`;
            } finally {
                submitButton.disabled = false;
                submitButton.value = 'Migrate Database';
            }
        });
    </script>
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


@app.route("/", methods=["GET"])
def index():
    return render_template_string(INDEX_HTML)


@app.route("/migrate", methods=["POST"])
def migrate():
    source_conn_str = request.form["source"]
    target_conn_str = request.form["target"]

    source_db = parse_db_string(source_conn_str)
    target_db = parse_db_string(target_conn_str)

    backup_file = "neon_backup.dump"

    def generate():
        try:
            # pg_dump
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
            yield f"Running pg_dump...\n$ {' '.join(dump_command)}\n\n"

            dump_process = subprocess.Popen(
                dump_command,
                env=dump_env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True,
            )
            for line in dump_process.stdout:
                yield line
            dump_process.wait()

            if dump_process.returncode != 0:
                yield "\npg_dump failed. Aborting migration."
                return

            yield "\npg_dump completed successfully.\n\n"

            # Create Schema
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
            yield f"Ensuring 'public' schema exists in target database...\n$ {' '.join(create_schema_command)}\n\n"

            create_schema_process = subprocess.Popen(
                create_schema_command,
                env=psql_env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True,
            )
            for line in create_schema_process.stdout:
                yield line
            create_schema_process.wait()

            if create_schema_process.returncode != 0:
                yield "\nWarning: Command to create schema failed, but continuing.\n\n"
            else:
                yield "Schema 'public' check/creation complete.\n\n"

            # pg_restore
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
            yield f"Running pg_restore...\n$ {' '.join(restore_command)}\n\n"

            restore_process = subprocess.Popen(
                restore_command,
                env=restore_env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True,
            )
            for line in restore_process.stdout:
                yield line
            restore_process.wait()

            if restore_process.returncode != 0:
                yield "\npg_restore finished with warnings (this is often acceptable for Neon migrations)."
            else:
                yield "\npg_restore completed successfully."

        except Exception as e:
            yield f"\nAn unhandled error occurred: {e}"
        finally:
            if os.path.exists(backup_file):
                os.remove(backup_file)

    return Response(generate(), mimetype="text/plain")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
