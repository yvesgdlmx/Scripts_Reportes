import csv
from datetime import datetime
import mysql.connector

# Configuración de la conexión a la base de datos
def connect_to_database():
    try:
        connection = mysql.connector.connect(
            host='autorack.proxy.rlwy.net',
            port=22723,
            user='root',
            password='zsulNCCrYFSfBqIxwwIXIKqLQKFJWwbw',
            database='railway'
        )
        connection.autocommit = True
        print("Conexión exitosa a la base de datos.")
        return connection
    except mysql.connector.Error as err:
        print(f"Error al conectar a la base de datos: {err}")
        return None

# Función para obtener la hora ajustada (para el registro, según las reglas previamente especificadas)
def get_record_hour():
    now = datetime.now()
    # Si el horario está entre 22:00 y 06:00, se toma la hora cerrada (minutos = 00)
    if now.hour >= 22 or now.hour < 6:
        return now.replace(minute=0, second=0).strftime("%H:%M:%S")
    else:
        hour = now.hour
        if now.minute < 30:
            hour -= 1
        if hour < 0:
            hour = 23
        return datetime.strptime(f"{hour}:30", "%H:%M").strftime("%H:%M:%S")

# Función para determinar si el momento actual es apto para insertar datos
def is_valid_time_to_insert():
    now = datetime.now()
    # Regla para el rango nocturno: 22:00 a 06:00, se omite inserción si minutos están entre 30 y 39
    if now.hour >= 22 or now.hour < 6:
        if 30 <= now.minute < 40:
            return False
    # Regla para el rango diurno: de 06:30 a 21:59, se omite inserción si minutos están entre 01 y 10
    else:
        # Para incluir correctamente el rango a partir de las 06:30
        if (now.hour > 6 or (now.hour == 6 and now.minute >= 30)) and now.hour < 22:
            if 1 <= now.minute < 11:
                return False
    return True

# Función para insertar datos en la tabla trabajos_enviados
def insert_jobs_data(cursor, fecha, hora, cliente, shipped_jobs, shipped_sales, average_sales, finished_jobs, semi_finished_jobs):
    try:
        sql_insert_jobs = """
        INSERT INTO trabajos_enviados (fecha, hora, cliente, shipped_jobs, shipped_sales, average_sales, finished_jobs, semi_finished_jobs)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(sql_insert_jobs, (fecha, hora, cliente, shipped_jobs, shipped_sales, average_sales, finished_jobs, semi_finished_jobs))
        print(f"Datos insertados para cliente {cliente}")
    except mysql.connector.Error as err:
        print(f"Error al insertar datos para cliente {cliente}: {err}")

# Función para procesar el archivo de trabajos enviados
def process_jobs_file(input_file, cursor):
    fecha_actual = datetime.now().date()  # Obtener la fecha actual
    hora_registro = get_record_hour()  # Hora según regla de redondeo o "cerrada"
    try:
        with open(input_file, 'r', encoding='utf-8') as file:
            reader = csv.reader(file, delimiter='\t')  # Usamos '\t' como delimitador
            next(reader)  # Saltar la primera línea (encabezados)
            for row in reader:
                # Elimina valores vacíos al final de la fila
                row = [value for value in row if value.strip() != ""]
                # Se valida que la fila tenga las columnas necesarias
                if len(row) == 6:
                    print("Procesando fila válida:", row)
                    cliente = row[0]
                    shipped_jobs = int(row[1])
                    shipped_sales = float(row[2])
                    average_sales = float(row[3])
                    finished_jobs = int(row[4])
                    semi_finished_jobs = int(row[5])
                    insert_jobs_data(cursor, fecha_actual, hora_registro, cliente, shipped_jobs, shipped_sales, average_sales, finished_jobs, semi_finished_jobs)
                else:
                    print(f"Fila inválida (cantidad de columnas no esperada): {row}")
    except Exception as e:
        print(f"Error al procesar el archivo: {e}")

# Función principal
def main():
    # Verifica si es un momento autorizado para insertar datos
    if not is_valid_time_to_insert():
        print("La ejecución actual está en un rango no permitido para la inserción de datos. Se omite la ejecución.")
        return

    input_file = 'I:/VISION/A_SHPYVE.txt'  # Ruta del archivo de entrada
    connection = connect_to_database()
    if connection:
        cursor = connection.cursor()
        process_jobs_file(input_file, cursor)
        cursor.close()
        connection.close()
        print("Conexión cerrada.")

# Ejecutar la función principal
if __name__ == "__main__":
    main()