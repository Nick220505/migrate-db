import json
import os

import psycopg2
from psycopg2 import sql
from tabulate import tabulate


def connect_to_db(connection_string):
    """Connect to the PostgreSQL database"""
    try:
        # Establecer explícitamente la codificación UTF-8 para la conexión
        conn = psycopg2.connect(connection_string)
        conn.set_client_encoding("UTF8")
        print("Connection to database successful")
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None


def get_tables(conn):
    """Get all table names in the public schema"""
    tables = []
    try:
        with conn.cursor() as cur:
            # Query to get all tables in the public schema
            cur.execute(
                """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """
            )
            tables = [row[0] for row in cur.fetchall()]
        return tables
    except Exception as e:
        print(f"Error getting tables: {e}")
        return tables


def get_table_schema(conn, table_name):
    """Get table schema information"""
    schema_info = []
    try:
        with conn.cursor() as cur:
            # Query to get column information
            cur.execute(
                """
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = 'public'
                AND table_name = %s
                ORDER BY ordinal_position
            """,
                (table_name,),
            )

            for row in cur.fetchall():
                schema_info.append(
                    {
                        "column_name": row[0],
                        "data_type": row[1],
                        "is_nullable": row[2],
                        "column_default": row[3],
                    }
                )

        return schema_info
    except Exception as e:
        print(f"Error getting table schema for {table_name}: {e}")
        return schema_info


def get_table_data(conn, table_name, limit=None):
    """Get data from a table with optional limit"""
    try:
        with conn.cursor() as cur:
            # Get columns
            cur.execute(f"SELECT * FROM {table_name} LIMIT 0")
            columns = [desc[0] for desc in cur.description]

            # Get data
            if limit:
                cur.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
            else:
                cur.execute(f"SELECT * FROM {table_name}")

            rows = cur.fetchall()

        return columns, rows
    except Exception as e:
        print(f"Error getting data from {table_name}: {e}")
        return [], []


def get_primary_keys(conn, table_name):
    """Get primary key column names for a table"""
    primary_keys = []
    try:
        with conn.cursor() as cur:
            # Query to get primary key columns
            cur.execute(
                """
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
                AND tc.table_schema = 'public'
                AND tc.table_name = %s
                ORDER BY kcu.ordinal_position
            """,
                (table_name,),
            )

            primary_keys = [row[0] for row in cur.fetchall()]

        return primary_keys
    except Exception as e:
        print(f"Error getting primary keys for {table_name}: {e}")
        return primary_keys


def get_foreign_keys(conn, table_name):
    """Get foreign key information for a table"""
    foreign_keys = []
    try:
        with conn.cursor() as cur:
            # Query to get foreign key constraints
            cur.execute(
                """
                SELECT
                    tc.constraint_name,
                    kcu.column_name,
                    ccu.table_name AS foreign_table,
                    ccu.column_name AS foreign_column
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage ccu
                    ON ccu.constraint_name = tc.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_schema = 'public'
                AND tc.table_name = %s
            """,
                (table_name,),
            )

            for row in cur.fetchall():
                foreign_keys.append(
                    {
                        "constraint_name": row[0],
                        "column_name": row[1],
                        "foreign_table": row[2],
                        "foreign_column": row[3],
                        "table_name": table_name,
                    }
                )

        return foreign_keys
    except Exception as e:
        print(f"Error getting foreign keys for {table_name}: {e}")
        return foreign_keys


def get_sequences(conn):
    """Get all sequences in the database"""
    sequences = []
    try:
        with conn.cursor() as cur:
            # Query to get sequence information
            cur.execute(
                """
                SELECT 
                    sequence_name,
                    data_type,
                    start_value,
                    minimum_value,
                    maximum_value,
                    increment
                FROM information_schema.sequences
                WHERE sequence_schema = 'public'
                ORDER BY sequence_name
            """
            )

            for row in cur.fetchall():
                sequences.append(
                    {
                        "sequence_name": row[0],
                        "data_type": row[1],
                        "start_value": row[2],
                        "min_value": row[3],
                        "max_value": row[4],
                        "increment": row[5],
                    }
                )

        return sequences
    except Exception as e:
        print(f"Error getting sequences: {e}")
        return sequences


def sql_value_formatter(value):
    """Format value for SQL INSERT statement"""
    if value is None:
        return "NULL"
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    elif isinstance(value, dict) or (
        isinstance(value, str) and value.startswith("{") and value.endswith("}")
    ):
        # Convertir el campo JSON a un literal de texto
        # Luego PostgreSQL lo convertirá a JSON usando el cast ::json
        import json

        if isinstance(value, dict):
            # Si es un diccionario, convertirlo a JSON
            json_str = json.dumps(value)
        else:
            # Si ya es una cadena, asegurarse de que sea JSON válido
            # Reemplazar comillas simples por comillas dobles
            json_str = value.replace("'", '"')
            # Validar y formatear el JSON
            try:
                # Intenta parsear y re-formatear el JSON
                parsed_json = json.loads(json_str)
                json_str = json.dumps(parsed_json)
            except:
                # Si hay error, dejarlo como texto plano
                pass

        # Escapar las comillas dobles para SQL
        escaped_str = json_str.replace("'", "''")
        return f"'{escaped_str}'::json"
    else:
        # Escape single quotes in strings
        return "'" + str(value).replace("'", "''") + "'"


def generate_create_table_sql(table_name, columns, primary_keys):
    """Generate SQL for CREATE TABLE statement"""
    sql = f"CREATE TABLE {table_name} (\n"

    # Add columns
    column_defs = []
    for col in columns:
        col_def = f"    {col['column_name']} {col['data_type']}"
        if col["column_default"]:
            col_def += f" DEFAULT {col['column_default']}"
        if col["is_nullable"] == "NO":
            col_def += " NOT NULL"
        column_defs.append(col_def)

    # Add primary key constraint
    if primary_keys:
        pk_columns = ", ".join(primary_keys)
        column_defs.append(f"    CONSTRAINT pk_{table_name} PRIMARY KEY ({pk_columns})")

    sql += ",\n".join(column_defs)
    sql += "\n);\n\n"

    return sql


def generate_foreign_key_sql(table_name, foreign_keys):
    """Generate SQL for foreign key constraints"""
    sql = ""
    for fk in foreign_keys:
        sql += f"ALTER TABLE {table_name} ADD CONSTRAINT {fk['constraint_name']} "
        sql += f"FOREIGN KEY ({fk['column_name']}) "
        sql += f"REFERENCES {fk['foreign_table']}({fk['foreign_column']});\n"
    sql += "\n"
    return sql


def generate_insert_sql(table_name, columns, rows):
    """Generate SQL INSERT statements for table data"""
    sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES\n"

    # Format values for each row
    values_list = []
    for row in rows:
        values = [sql_value_formatter(value) for value in row]
        values_list.append(f"({', '.join(values)})")

    sql += ",\n".join(values_list) + ";\n\n"
    return sql


def generate_sequence_sql(sequences):
    """Generate SQL for creating sequences"""
    sequence_sql = ""
    for seq in sequences:
        sequence_sql += f"CREATE SEQUENCE {seq['sequence_name']}\n"
        sequence_sql += f"    INCREMENT {seq['increment']}\n"
        sequence_sql += f"    START {seq['start_value']}\n"
        sequence_sql += f"    MINVALUE {seq['min_value']}\n"
        sequence_sql += f"    MAXVALUE {seq['max_value']}\n"
        if seq["data_type"]:
            sequence_sql += f"    AS {seq['data_type']}"
        sequence_sql += ";\n\n"
    return sequence_sql


def write_file(file_path, content):
    """Write content to a file with UTF-8 encoding"""
    # Crear el directorio si no existe
    directory = os.path.dirname(file_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)

    # Escribir el archivo con codificación UTF-8
    with open(file_path, "w", encoding="utf-8") as file:
        file.write(content)


def export_schema_and_data_sql(
    connection_string, output_dir="sql_export", sample_data=False
):
    """Extract and export schema and data as SQL scripts"""
    conn = connect_to_db(connection_string)
    if not conn:
        return

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Get all tables
    tables = get_tables(conn)
    print(f"Found {len(tables)} tables: {', '.join(tables)}")

    # Get all sequences
    sequences = get_sequences(conn)

    # Create schema file
    schema_file = os.path.join(output_dir, "01_schema.sql")
    schema_content = "-- Database Schema Export\n"
    schema_content += "-- Generated by extract_db_schema.py\n\n"

    # Add sequences
    if sequences:
        schema_content += "-- Sequences\n"
        schema_content += generate_sequence_sql(sequences)

    # Add create table statements
    for table_name in tables:
        print(f"\nProcessing table schema: {table_name}")

        # Get table schema
        schema = get_table_schema(conn, table_name)

        # Get primary keys
        primary_keys = get_primary_keys(conn, table_name)

        # Generate and write CREATE TABLE statement
        schema_content += f"-- Table: {table_name}\n"
        create_table_sql = generate_create_table_sql(table_name, schema, primary_keys)
        schema_content += create_table_sql

    # Write schema file with UTF-8 encoding
    write_file(schema_file, schema_content)

    # Create foreign keys file
    fk_file = os.path.join(output_dir, "02_foreign_keys.sql")
    fk_content = "-- Foreign Key Constraints\n"
    fk_content += "-- Generated by extract_db_schema.py\n\n"

    # Process each table
    for table_name in tables:
        # Get foreign keys
        foreign_keys = get_foreign_keys(conn, table_name)

        if foreign_keys:
            fk_content += f"-- Foreign keys for table: {table_name}\n"
            fk_sql = generate_foreign_key_sql(table_name, foreign_keys)
            fk_content += fk_sql

    # Write foreign keys file with UTF-8 encoding
    write_file(fk_file, fk_content)

    # Create data file
    data_file = os.path.join(output_dir, "03_data.sql")
    data_content = "-- Data Export\n"
    data_content += "-- Generated by extract_db_schema.py\n\n"

    # Process each table for data
    for table_name in tables:
        print(f"Processing table data: {table_name}")

        # Get table data (with limit if sample data is requested)
        limit = 10 if sample_data else None
        columns, rows = get_table_data(conn, table_name, limit=limit)

        if rows:
            data_content += f"-- Data for table: {table_name}\n"
            data_content += f"-- {len(rows)} rows\n"
            insert_sql = generate_insert_sql(table_name, columns, rows)
            data_content += insert_sql
        else:
            data_content += f"-- No data for table: {table_name}\n\n"

    # Write data file with UTF-8 encoding
    write_file(data_file, data_content)

    # Print tables schema information
    for table_name in tables:
        print(f"\nTable: {table_name}")

        # Get table schema
        schema = get_table_schema(conn, table_name)
        print("Schema:")
        headers = ["Column", "Type", "Nullable", "Default"]
        table_data = [
            [s["column_name"], s["data_type"], s["is_nullable"], s["column_default"]]
            for s in schema
        ]
        print(tabulate(table_data, headers=headers, tablefmt="grid"))

        # Get primary keys
        primary_keys = get_primary_keys(conn, table_name)
        if primary_keys:
            print(f"Primary Keys: {', '.join(primary_keys)}")

    # Close connection
    conn.close()
    print(f"\nSQL export completed. Files saved to '{output_dir}' directory")
    print(f"Generated files:")
    print(f"  - {schema_file} (Database schema)")
    print(f"  - {fk_file} (Foreign key constraints)")
    print(f"  - {data_file} (Table data)")


if __name__ == "__main__":
    # Connection string to Neon PostgreSQL
    connection_string = "postgresql://neondb_owner:npg_oSnbAJiw3Wz8@ep-curly-moon-a4xh55ke-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require"

    # Export schema and data as SQL
    export_schema_and_data_sql(connection_string, sample_data=False)
