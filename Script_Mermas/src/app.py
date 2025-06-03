import csv
import glob
import os
import mysql.connector
from datetime import datetime, timedelta, time
# ================================================================
# Función para validar si un registro tiene una hora "madura" para procesar.
# Se asume que se procesan solo las horas ya transcurridas (menores o iguales a la actual)
# ================================================================
def is_valid_time(final_time_str):
    try:
        record_hour = int(final_time_str.split(":")[0])
        current_hour = datetime.now().hour
        if record_hour > current_hour:
            return False
        return True
    except Exception as e:
        print(f"Error al validar el tiempo '{final_time_str}': {e}")
        return False
# ================================================================
# Función que, dado un datetime (registro original), retorna la marca de tiempo
# ajustada según el turno y la corrección en el límite de 21:30.
# ================================================================
def get_final_time(dt_original):
    t = dt_original.time()
    if time(6, 0) <= t < time(6, 30):
        return "06:30:00"
    elif time(6, 30) <= t < time(21, 30):
        if dt_original.minute < 30:
            final_hour = dt_original.hour - 1
        else:
            final_hour = dt_original.hour
        return f"{final_hour:02d}:30:00"
    else:
        if dt_original.hour == 21 and dt_original.minute >= 30:
            return "22:00:00"
        else:
            return f"{dt_original.hour:02d}:00:00"
# ================================================================
# Función para procesar los datos de la columna TIME para la tabla conteo_mermas
# Se omiten las filas cuyo valor en la columna "part" (índice 8) sea "frame"
# ================================================================
def process_time_data(filename):
    grupos = {}
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            reader = csv.reader(file, delimiter=',')
            header = next(reader)  # omite la cabecera
            for row in reader:
                # Validación: si existe la columna "part" (índice 8) y su valor es "frame", se salta el registro.
                if len(row) > 8 and row[8].strip().lower() == "frame":
                    continue
                # Filtrar registros que contengan MERMA DE ARMAZON o REWORK en department (se asume índice 4)
                if len(row) > 4:
                    department = row[4].strip()
                    if department.upper() in ["MERMA DE ARMAZON", "REWORK"]:
                        continue
                if len(row) > 3:
                    time_str = row[3].strip()
                    try:
                        dt_original = datetime.strptime(time_str, '%I:%M%p')
                    except Exception as e:
                        print(f"Error al parsear '{time_str}': {e}")
                        continue
                    final_time = get_final_time(dt_original)
                    if not is_valid_time(final_time):
                        continue
                    grupos[final_time] = grupos.get(final_time, 0) + 1
    except Exception as e:
        print(f"Error al procesar el archivo para datos de hora: {e}")
    fecha_actual = datetime.now().strftime('%Y-%m-%d')
    registros_agrupados = [(fecha_actual, hora, total) for hora, total in grupos.items()]
    return registros_agrupados
# ================================================================
# Función para procesar los datos de Reason para la tabla conteo_razones_mermas
# Se agregan las columnas TrayNumber, Department, Position y Part.
# La agrupación se hará sobre (hora, reason, traynumber, department, position) y se irán concatenando los valores de part.
# Se omiten los registros cuyo valor en la columna "part" sea "frame"
# ================================================================
def process_reason_data(filename):
    grupos = {}
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            reader = csv.reader(file, delimiter=',')
            header = next(reader)  # asumimos que la cabecera contiene los nombres de columna
            for row in reader:
                # Verificar que existan al menos 9 columnas (índices 0 a 8)
                if len(row) > 8:
                    # Si el valor de la columna "part" es "frame", se ignora el registro.
                    if row[8].strip().lower() == "frame":
                        continue
                    # Procesar y filtrar por Department.
                    department = row[4].strip()
                    # Convertimos a mayúsculas para hacer una comparación case-insensitive.
                    if department.upper() in ["MERMA DE ARMAZON", "REWORK"]:
                        continue
                    time_str   = row[3].strip()   # TIME (índice 3)
                    traynumber = row[1].strip()   # TRAYNUM (índice 1)
                    # Si traynumber está vacío, asignar el valor "0"
                    if traynumber == "":
                        traynumber = "0"
                    position   = row[5].strip()   # Position (índice 5)
                    reason     = row[6].strip()   # Reason (índice 6)
                    part       = row[8].strip()   # Part (índice 8)
                    try:
                        dt_original = datetime.strptime(time_str, '%I:%M%p')
                    except Exception as e:
                        print(f"Error al parsear '{time_str}': {e}")
                        continue
                    final_time = get_final_time(dt_original)
                    if not is_valid_time(final_time):
                        continue
                    # Agrupamos por la combinación: (final_time, reason, traynumber, department, position)
                    key = (final_time, reason, traynumber, department, position)
                    if key not in grupos:
                        grupos[key] = {"count": 0, "parts": set()}
                    grupos[key]["count"] += 1
                    grupos[key]["parts"].add(part)
    except Exception as e:
        print(f"Error al procesar el archivo para datos de razón: {e}")
    fecha_actual = datetime.now().strftime('%Y-%m-%d')
    registros = []
    for (final_time, reason, traynumber, department, position), info in grupos.items():
        total = info["count"]
        # Concatenar los diferentes valores de part separados por coma
        parts_str = ",".join(sorted(info["parts"]))
        registros.append((fecha_actual, final_time, reason, total, traynumber, department, position, parts_str))
    return registros
# ================================================================
# Búsqueda automática del archivo CSV a procesar
# ================================================================
data_dir = 'I:/SANDBOX/'
file_pattern = os.path.join(data_dir, 'mkbrk*.csv')
matching_files = glob.glob(file_pattern)
if matching_files:
    data_file = max(matching_files, key=os.path.getctime)
    print("Se encontró el archivo:", data_file)
else:
    print("No se encontró ningún archivo que cumpla con el patrón especificado.")
    exit()
# ================================================================
# Conexión y operaciones en la base de datos
# ================================================================
try:
    connection = mysql.connector.connect(
        host='autorack.proxy.rlwy.net',
        port=22723,
        user='root',
        password='zsulNCCrYFSfBqIxwwIXIKqLQKFJWwbw',
        database='railway'
    )
    if connection.is_connected():
        print("Conexión establecida exitosamente.")
    cursor = connection.cursor()
    
    # 1. Procesamiento e inserción/actualización en la tabla "conteo_mermas"
    time_records = process_time_data(data_file)
    print("Registros procesados para conteo_mermas:", time_records)
    sql_insert_time = """
    INSERT INTO conteo_mermas (fecha, hora, total)
    VALUES (%s, %s, %s)
    ON DUPLICATE KEY UPDATE total = IF(VALUES(total) > total, VALUES(total), total)
    """
    if time_records:
        cursor.executemany(sql_insert_time, time_records)
        connection.commit()
        print(f"Se insertaron/actualizaron {cursor.rowcount} registros en conteo_mermas")
    else:
        print("No se encontraron registros para insertar en conteo_mermas.")
        
    # 2. Procesamiento e inserción/actualización en la tabla "conteo_razones_mermas"
    reason_records = process_reason_data(data_file)
    print("Registros procesados para conteo_razones_mermas:", reason_records)
    sql_insert_reason = """
    INSERT INTO conteo_razones_mermas (fecha, hora, razon, total, trayNumber, department, position, part)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE total = IF(VALUES(total) > total, VALUES(total), total)
    """
    if reason_records:
        cursor.executemany(sql_insert_reason, reason_records)
        connection.commit()
        print(f"Se insertaron/actualizaron {cursor.rowcount} registros en conteo_razones_mermas")
    else:
        print("No se encontraron registros para insertar en conteo_razones_mermas.")
        
except mysql.connector.Error as error:
    print(f"Error en la base de datos: {error}")
except Exception as ex:
    print(f"Error en el script: {ex}")
finally:
    if 'connection' in locals() and connection.is_connected():
        cursor.close()
        connection.close()
        print("Conexión cerrada para el script")