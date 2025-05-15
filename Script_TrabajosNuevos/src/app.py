import csv
import mysql.connector
from datetime import datetime
def parse_date(date_str):
    # Primero se intentan los formatos mes/día/año
    date_formats = [
        '%m/%d/%Y',    # 04/01/2025
        '%m/%d/%y',    # 04/01/25
        '%d/%m/%Y',    # 13/11/2024
        '%d/%m/%y',    # 13/11/24
        '%Y-%m-%d'     # 2024-11-13
    ]
    for fmt in date_formats:
        try:
            date = datetime.strptime(date_str, fmt)
            # Si el año es de dos dígitos y se parseó como inferior a 100, se asume que es mayor a 2000
            if date.year < 100:
                date = date.replace(year=date.year + 2000)
            return date.strftime('%Y-%m-%d')
        except ValueError:
            continue
    raise ValueError(f"No se pudo parsear la fecha: {date_str}")
def get_record_hour():
    now = datetime.now()
    hour = now.hour
    if now.minute < 30:
        # Si los minutos son menores a 30, usamos la hora anterior con :30
        hour -= 1
    # Asegurarse de que la hora no sea negativa (por ejemplo, a medianoche)
    if hour < 0:
        hour = 23
    return datetime.strptime(f"{hour}:30", "%H:%M").strftime("%H:%M:%S")
def process_new_jobs(input_file):
    data_to_insert = []
    with open(input_file, 'r') as file:
        reader = csv.reader(file, delimiter='\t')
        next(reader)  # Saltar la primera línea (encabezados)
        for row in reader:
            if len(row) >= 13:
                try:
                    fecha = parse_date(row[0])
                    hora = get_record_hour()
                    total_new_jobs = int(row[1])
                    ink_jobs = int(row[2])
                    ink_no_ar = int(row[3])
                    ink_ar = int(row[4])
                    hoya_jobs = int(row[5])
                    hoya_no_ar = int(row[6])
                    hoya_ar = int(row[7])
                    nvi_jobs = int(row[8])
                    nvi_no_ar = int(row[9])
                    nvi_ar = int(row[10])
                    terminado = int(row[11])
                    semi_term = int(row[12])
                    data_to_insert.append((
                        fecha, hora, total_new_jobs, ink_jobs, ink_no_ar, ink_ar, 
                        hoya_jobs, hoya_no_ar, hoya_ar, nvi_jobs, nvi_no_ar, 
                        nvi_ar, terminado, semi_term
                    ))
                except ValueError as e:
                    print(f"Error al procesar la fila: {e}")
                    print(f"Fila problemática: {row}")
                    continue
    return data_to_insert
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
    input_file = 'I:/VISION/A_INARCC.txt'
    data_to_insert = process_new_jobs(input_file)
    
    # Filtrar para obtener solo el/los registros con la fecha más reciente
    if data_to_insert:
        # Convertir el campo fecha a objeto datetime y obtener el máximo:
        max_date = max(datetime.strptime(record[0], "%Y-%m-%d") for record in data_to_insert)
        # Solo dejamos aquellos registros cuya fecha es igual a la fecha máxima encontrada
        data_to_insert = [
            record for record in data_to_insert 
            if datetime.strptime(record[0], "%Y-%m-%d") == max_date
        ]
    else:
        print("No se encontraron registros válidos en el archivo.")
    sql_insert = """
    INSERT INTO trabajos_nuevos (
        fecha, hora, total_new_jobs, ink_jobs, ink_no_ar, ink_ar, 
        hoya_jobs, hoya_no_ar, hoya_ar, nvi_jobs, nvi_no_ar, 
        nvi_ar, terminado, semi_term
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    registros_insertados = 0
    for record in data_to_insert:
        # Verificar si el registro ya existe
        cursor.execute(
            "SELECT COUNT(*) FROM trabajos_nuevos WHERE fecha = %s AND hora = %s", 
            (record[0], record[1])
        )
        if cursor.fetchone()[0] == 0:
            cursor.execute(sql_insert, record)
            registros_insertados += 1
    connection.commit()
    print(f"Número de registros insertados: {registros_insertados}")
    print("Datos insertados exitosamente.")
except mysql.connector.Error as err:
    print("Error al ejecutar el comando SQL:", err)
finally:
    if 'cursor' in locals() and cursor:
        cursor.close()
    if 'connection' in locals() and connection.is_connected():
        connection.close()
        print("Conexión cerrada.")
print("Proceso completado.")