import csv
import glob
import os
import mysql.connector
from datetime import datetime, timedelta
# ================================================================
# Función para validar si un registro tiene una hora "madura" para procesar.
# Se asume que se procesan solo las horas ya transcurridas (menores o iguales a la actual)
# ================================================================
def is_valid_time(final_time):
    try:
        # Extraemos la hora (dos primeros dígitos del string "HH:MM:SS")
        record_hour = int(final_time.split(":")[0])
        current_hour = datetime.now().hour
        if record_hour > current_hour:
            return False
        return True
    except Exception as e:
        print(f"Error al validar el tiempo '{final_time}': {e}")
        return False
# ================================================================
# Función para procesar los datos de la columna TIME para la tabla conteo_mermas
# ================================================================
def process_time_data(filename):
    """
    Procesa el archivo CSV para leer la columna TIME (índice 3) de cada registro y:
      1. Parsea la hora en formato de 12 horas (ej. "12:34AM").
      2. Resta una hora al valor leído.
      3. Según la hora original asigna los minutos:
            - Si la hora original está entre 06:00 y 21:59 se asigna 30.
            - De lo contrario se asigna 00.
      4. Forma la cadena final en formato "HH:MM:SS" basada en la hora ajustada.
      5. Verifica que la hora final sea válida (ya transcurrida) según la hora actual.
      6. Acumula el total por hora.
    Retorna una lista de tuplas con (fecha_actual, hora_final, total_por_hora).
    """
    grupos = {}
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            reader = csv.reader(file, delimiter=',')
            header = next(reader)  # omite la cabecera
            for row in reader:
                if len(row) > 3:
                    time_str = row[3].strip()
                    try:
                        dt_original = datetime.strptime(time_str, '%I:%M%p')
                    except Exception as e:
                        print(f"Error al parsear '{time_str}': {e}")
                        continue
                    # Restar una hora al registro original
                    dt_adjusted = dt_original - timedelta(hours=1)
                    if 6 <= dt_original.hour < 22:
                        minutos = 30
                    else:
                        minutos = 0
                    final_time = f"{dt_adjusted.hour:02d}:{minutos:02d}:00"
                    
                    # Sólo se procesan registros de hora que ya pasaron
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
# ================================================================
def process_reason_data(filename):
    """
    Procesa el archivo CSV para leer:
      - Columna TIME (índice 3)
      - Columna Reason (índice 6)
    Realiza lo siguiente para cada registro:
      1. Parsea la hora en formato de 12 horas (ej. "12:34AM").
      2. Resta una hora al valor leído.
      3. Según la hora original asigna los minutos:
           - Si la hora original está entre 06:00 y 21:59 se asigna 30.
           - De lo contrario se asigna 00.
      4. Forma la cadena final en formato "HH:MM:SS".
      5. Verifica que la hora final sea válida (ya transcurrida).
      6. Agrupa el registro por la combinación de (hora_final, reason).
    Retorna una lista de tuplas con (fecha_actual, hora_final, razon, total_por_combinación).
    """
    grupos = {}
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            reader = csv.reader(file, delimiter=',')
            header = next(reader)
            for row in reader:
                if len(row) > 6:
                    time_str = row[3].strip()
                    reason = row[6].strip()
                    try:
                        dt_original = datetime.strptime(time_str, '%I:%M%p')
                    except Exception as e:
                        print(f"Error al parsear '{time_str}': {e}")
                        continue
                    dt_adjusted = dt_original - timedelta(hours=1)
                    if 6 <= dt_original.hour < 22:
                        minutos = 30
                    else:
                        minutos = 0
                    final_time = f"{dt_adjusted.hour:02d}:{minutos:02d}:00"
                    
                    if not is_valid_time(final_time):
                        continue
                    key = (final_time, reason)
                    grupos[key] = grupos.get(key, 0) + 1
    except Exception as e:
        print(f"Error al procesar el archivo para datos de razón: {e}")
    fecha_actual = datetime.now().strftime('%Y-%m-%d')
    registros = [(fecha_actual, final_time, reason, total) for (final_time, reason), total in grupos.items()]
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
    # Aquí se asume que la PK de conteo_mermas es (fecha, hora).
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
    # Se asume que la PK de conteo_razones_mermas es (fecha, hora, razon).
    sql_insert_reason = """
    INSERT INTO conteo_razones_mermas (fecha, hora, razon, total)
    VALUES (%s, %s, %s, %s)
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