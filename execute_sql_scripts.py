import os
import re
import time

import psycopg2


def execute_sql_scripts(
    connection_string, scripts_folder="sql_export", drop_existing=True
):
    """
    Ejecuta los scripts SQL generados en la base de datos especificada por la cadena de conexión

    Args:
        connection_string (str): Cadena de conexión a PostgreSQL
        scripts_folder (str): Carpeta donde se encuentran los scripts SQL
        drop_existing (bool): Si es True, elimina las tablas existentes antes de crear nuevas
    """
    print("Iniciando la ejecución de scripts SQL...")

    # Conectar a la base de datos
    try:
        conn = psycopg2.connect(connection_string)
        # Establecer explícitamente la codificación UTF-8
        conn.set_client_encoding("UTF8")
        # Activar el modo autocommit para poder ejecutar DROP sin estar en transacción
        conn.autocommit = True
        print("Conexión a la base de datos exitosa")
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")
        return False

    cursor = conn.cursor()

    # Si drop_existing es True, eliminar todas las tablas existentes
    if drop_existing:
        try:
            print("\nEliminando objetos existentes en la base de datos...")

            # Obtener todas las tablas de la base de datos
            cursor.execute(
                """
                SELECT tablename FROM pg_tables 
                WHERE schemaname = 'public';
            """
            )
            tables = cursor.fetchall()

            # Eliminar todas las tablas utilizando CASCADE para eliminar dependencias
            for table in tables:
                table_name = table[0]
                print(f"  Eliminando tabla: {table_name}")
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
                except Exception as table_error:
                    print(f"    Error al eliminar la tabla {table_name}: {table_error}")

            # Obtener todas las secuencias de la base de datos
            cursor.execute(
                """
                SELECT sequence_name FROM information_schema.sequences
                WHERE sequence_schema = 'public';
            """
            )
            sequences = cursor.fetchall()

            # Eliminar todas las secuencias
            for seq in sequences:
                seq_name = seq[0]
                print(f"  Eliminando secuencia: {seq_name}")
                try:
                    cursor.execute(f"DROP SEQUENCE IF EXISTS {seq_name} CASCADE;")
                except Exception as seq_error:
                    print(f"    Error al eliminar la secuencia {seq_name}: {seq_error}")

            print("Limpieza completada con éxito.\n")
        except Exception as e:
            print(f"Error durante la limpieza de la base de datos: {e}")
            # No hacemos rollback porque autocommit está activado

    # Desactivar autocommit para las operaciones siguientes (transacciones)
    conn.autocommit = False

    # Lista de scripts a ejecutar en orden
    script_files = [
        "01_schema.sql",  # Crear tablas
        "02_foreign_keys.sql",  # Añadir claves foráneas
        "03_data.sql",  # Insertar datos
    ]

    success = True

    for script_file in script_files:
        script_path = os.path.join(scripts_folder, script_file)

        if not os.path.exists(script_path):
            print(f"⚠ El archivo {script_path} no existe.")
            continue

        print(f"\nEjecutando {script_file}...")
        start_time = time.time()

        try:
            # Leer el contenido del script
            with open(script_path, "r", encoding="utf-8") as f:
                script_content = f.read()

            # Dividir el script en instrucciones individuales
            # Consideramos que cada instrucción termina con un punto y coma
            statements = re.split(r";(?=(?:[^\']*\'[^\']*\')*[^\']*$)", script_content)

            # Filtrar statements vacíos y comentarios
            valid_statements = []
            for stmt in statements:
                # Eliminar comentarios y verificar si queda algo
                lines = []
                for line in stmt.split("\n"):
                    if not line.strip().startswith("--"):
                        lines.append(line)
                clean_stmt = "\n".join(lines).strip()
                if clean_stmt:
                    valid_statements.append(clean_stmt)

            if not valid_statements:
                print(
                    f"  ⚠ El archivo {script_file} no contiene instrucciones SQL válidas. Continuando..."
                )
                continue

            print(f"  Se encontraron {len(valid_statements)} instrucciones válidas.")

            # Ejecutar cada instrucción por separado
            for i, statement in enumerate(valid_statements):
                try:
                    cursor.execute(statement)
                except Exception as e:
                    print(f"\nError en la sentencia #{i+1}:\n{statement[:200]}...")
                    raise e

            # Confirmar los cambios
            conn.commit()
            end_time = time.time()
            elapsed_time = end_time - start_time
            print(
                f"✓ {script_file} ejecutado exitosamente. (tiempo: {elapsed_time:.2f} segundos)"
            )

        except Exception as e:
            # Revertir los cambios en caso de error
            conn.rollback()
            print(f"✗ Error al ejecutar {script_file}: {e}")

            # Cerrar conexión y salir
            cursor.close()
            conn.close()
            print("\nConexión a la base de datos cerrada")
            print("\n⚠ La ejecución de scripts se ha detenido debido a un error.")
            success = False
            break

    # Cerrar la conexión
    cursor.close()
    conn.close()
    print("\nConexión a la base de datos cerrada")

    if success:
        print("\n✓ Todos los scripts se han ejecutado exitosamente.")
        return True
    else:
        print("\n⚠ La ejecución de scripts se ha detenido debido a un error.")
        return False


if __name__ == "__main__":
    # Cadena de conexión a la base de datos destino
    connection_string = "postgresql://neondb_owner:npg_luUSomPsi0Z3@ep-sweet-glade-a4e3sh9j-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require"

    # Ejecutar los scripts SQL
    execute_sql_scripts(connection_string)
