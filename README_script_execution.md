# Programas para Exportar e Importar Base de Datos PostgreSQL

Este conjunto de programas permite extraer el esquema y datos de una base de datos PostgreSQL y restaurarlos en otra base de datos.

## Archivos incluidos

- `extract_db_schema.py`: Extrae el esquema y datos de una base de datos PostgreSQL y genera scripts SQL.
- `execute_sql_scripts.py`: Ejecuta los scripts SQL generados en otra base de datos PostgreSQL.

## Requisitos

```
pip install psycopg2-binary
```

## Pasos para exportar e importar la base de datos

### 1. Extraer el esquema y datos

Para extraer el esquema y datos de la base de datos origen, ejecute:

```
python extract_db_schema.py
```

Este programa:
- Se conecta a la base de datos usando la cadena de conexión configurada
- Extrae todas las tablas, secuencias, claves primarias y foráneas
- Extrae todos los datos de las tablas
- Genera tres archivos SQL en la carpeta `sql_export`:
  - `01_schema.sql`: Contiene los comandos CREATE para tablas y secuencias
  - `02_foreign_keys.sql`: Contiene los comandos ALTER TABLE para añadir claves foráneas
  - `03_data.sql`: Contiene los comandos INSERT con todos los datos

### 2. Importar en la base de datos destino

Para importar los datos en una nueva base de datos, ejecute:

```
python execute_sql_scripts.py
```

Este programa:
- Se conecta a la base de datos destino usando la cadena de conexión configurada
- Ejecuta los tres scripts SQL generados en el paso anterior en el orden correcto
- Muestra el progreso y maneja posibles errores durante la ejecución

## Personalización

Para usar con diferentes bases de datos, modifique la cadena de conexión en cada programa:

- En `extract_db_schema.py`, modifique la variable `connection_string` para la base de datos origen
- En `execute_sql_scripts.py`, modifique la variable `connection_string` para la base de datos destino

## Características

- Preserva la codificación UTF-8 para manejar caracteres especiales correctamente
- Ejecuta las sentencias SQL individualmente para mejor manejo de errores
- Muestra el progreso durante la ejecución de scripts grandes
- Manejo de transacciones para garantizar consistencia (commit/rollback) 