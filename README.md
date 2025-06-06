# Extractor de Esquema y Datos de PostgreSQL a SQL

Este programa permite extraer el esquema completo y datos de una base de datos PostgreSQL (Neon) y generar scripts SQL para recrear la base de datos. El programa genera:

- Scripts SQL para crear tablas, secuencias y restricciones
- Scripts SQL para insertar los datos existentes
- Información detallada sobre la estructura de la base de datos

## Requisitos

Instala las dependencias necesarias con:

```
pip install -r requirements.txt
```

## Uso

El script ya tiene configurada la cadena de conexión a la base de datos Neon:

```python
connection_string = "postgresql://neondb_owner:npg_oSnbAJiw3Wz8@ep-curly-moon-a4xh55ke-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require"
```

Para ejecutar el programa:

```
python extract_db_schema.py
```

## Salida

El programa genera los siguientes resultados:

1. Información en consola sobre cada tabla, incluyendo:
   - Esquema (columnas, tipos de datos, etc.)
   - Claves primarias
   - Claves foráneas
   - Datos de muestra (primeras 10 filas)

2. Archivos SQL en la carpeta `sql_export`:
   - `01_schema.sql` - Contiene las instrucciones CREATE para secuencias y tablas
   - `02_foreign_keys.sql` - Contiene las instrucciones ALTER TABLE para claves foráneas
   - `03_data.sql` - Contiene las instrucciones INSERT para los datos de las tablas

## Personalización

- Para exportar todos los datos en lugar de solo muestras, modifica el parámetro `sample_data` a `False` en la llamada a `export_schema_and_data_sql`
- Para cambiar el directorio de salida, modifica el parámetro `output_dir` en la llamada a `export_schema_and_data_sql` 