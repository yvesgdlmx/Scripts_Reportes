import csv
import mysql.connector
from datetime import datetime
# Diccionario de traducción de motivos de cancelación
translation_dict = {
    "Doctor cancel": "Otros",
    "Duplicate order": "Trabajo duplicado",
    "Coat / Tint selection": "Tinte",
    "Product not available": "Falta de mica",
    "Insurance denied": "Otros",
    "Power out of range": "Fuera de rango",
    "Cut problem": "Problemas de corte",
    "Edge / mounting problem": "Problemas de corte",
    "Missing fitting values": "Otros",
    "Broken, bad, or missing frame": "Falta de armazon",
    "Warranty expired": "Otros",
    "Test job": "Trabajo de prueba",
    "Resubmit": "Otros",
    "Warranty rejected / disqualified": "Otros",
    "Non-activity": "Sin actividad",
    "": "Falta de mica"  # Razón vacía se traduce a "Falta de mica"
}
# Función para procesar el archivo wipnhi
def process_wipnhi_file(input_file):
    count_nvi = 0
    count_hoya = 0
    count_ink = 0
    with open(input_file, 'r') as file:
        reader = csv.reader(file, delimiter='\t')
        next(reader)  # Saltar la primera línea (encabezados)
        for row in reader:
            if len(row) > 0:
                client_code = row[0]
                if client_code == '99999':
                    count_nvi += 1
                elif client_code == '22222':
                    count_hoya += 1
                elif client_code == '55555':
                    count_ink += 1
    return count_nvi, count_hoya, count_ink
# Función para procesar el archivo inarcc
def process_inarcc_file(input_file):
    total_ink = 0
    total_hoya = 0
    total_nvi_jobs = 0
    total_finished_nvi = 0
    with open(input_file, 'r') as file:
        reader = csv.reader(file, delimiter='\t')
        next(reader)  # Saltar la línea de encabezados
        for row in reader:
            if len(row) >= 13:  # Verificar que existan las columnas necesarias
                try:
                    # Convertir y acumular los valores
                    total_ink += int(row[2])
                    total_hoya += int(row[5])
                    total_nvi_jobs += int(row[8])
                    total_finished_nvi += int(row[11])
                except ValueError as e:
                    print(f"Error al procesar la fila: {e}")
                    print(f"Fila problemática: {row}")
                    continue
    # Calcular semifinish_nvi como la diferencia total de NVI Jobs - Terminado
    semifinish_nvi = total_nvi_jobs - total_finished_nvi
    # Empaquetar los totales en una sola tupla
    data_to_insert = [(total_nvi_jobs, total_ink, total_hoya, total_finished_nvi, semifinish_nvi)]
    return data_to_insert
# Función para procesar el archivo enviados
def process_enviados_file(input_file):
    total_nvi = 0
    total_ink = 0
    total_hoya = 0
    total_semifinish = 0
    total_finished = 0
    with open(input_file, 'r') as file:
        reader = csv.reader(file, delimiter='\t')
        next(reader, None)  # Saltar la primera línea (encabezados)
        data_found = False
        for row in reader:
            if len(row) >= 3:
                data_found = True
                job_type = row[0]
                job_status = row[1]
                try:
                    job_count = int(row[2])
                except ValueError:
                    continue  # Ignorar registros no válidos
                if job_type == 'NV':
                    total_nvi += job_count
                    if job_status == 'S':
                        total_semifinish += job_count
                    elif job_status == 'F':
                        total_finished += job_count
                elif job_type == 'INK':
                    total_ink += job_count
                elif job_type == 'HO':
                    total_hoya += job_count
    if not data_found:
        print("Archivo enviados sin registros válidos, insertando ceros.")
    return total_nvi, total_ink, total_hoya, total_semifinish, total_finished
# Función para procesar el archivo cancelados
def process_cancelados_file(input_file):
    total_nvi = 0
    total_ink = 0
    total_hoya = 0
    with open(input_file, 'r') as file:
        reader = csv.reader(file, delimiter='\t')
        next(reader)  # Saltar la primera línea (encabezados)
        for row in reader:
            if len(row) >= 3:
                job_type = row[0]
                job_count = int(row[2])
                if job_type == 'NV':
                    total_nvi += job_count
                elif job_type == 'INK':
                    total_ink += job_count
                elif job_type == 'HO':
                    total_hoya += job_count
    return total_nvi, total_ink, total_hoya
# Función para procesar el nuevo archivo con traducción
def process_new_file_with_translation(input_file):
    data_to_insert = []
    with open(input_file, 'r') as file:
        reader = csv.reader(file, delimiter='\t')
        next(reader, None)  # Saltar la primera línea si tiene encabezados
        for row in reader:
            if len(row) >= 4:
                job_type = row[0]
                job_category = row[1]
                job_issue_english = row[2]
                job_issue_spanish = translation_dict.get(job_issue_english, "Otros")  # Traducir motivo
                try:
                    job_count = int(row[3])
                except ValueError:
                    continue  # Ignorar registros no válidos
                data_to_insert.append((job_type, job_category, job_issue_spanish, job_count))
    return data_to_insert
# Verificamos que el script no se ejecute los domingos.
if __name__ == '__main__':
    from datetime import datetime
    # Usando isoweekday(), domingo es 7.
    if datetime.now().isoweekday() == 7:
        print("Hoy es domingo. El script no se ejecutará.")
    else:
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
            fecha_actual = datetime.now().strftime('%Y-%m-%d')
            
            # Procesar archivo wipnhi
            input_file_wipnhi = 'I:/VISION/a_WIPNHI.txt'
            count_nvi, count_hoya, count_ink = process_wipnhi_file(input_file_wipnhi)
            sql_insert_datos = """
            INSERT INTO wip_totals (
                fecha, total_nvi, total_ink, total_hoya, accion, semifinish_nvi, finished_nvi
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql_insert_datos, (fecha_actual, count_nvi, count_ink, count_hoya, 'wip total', None, None))
            
            # Procesar archivo inarcc
            input_file_inarcc = 'I:/VISION/A_INARCC.txt'
            data_to_insert_inarcc = process_inarcc_file(input_file_inarcc)
            for record in data_to_insert_inarcc:
                cursor.execute(sql_insert_datos, (fecha_actual, record[0], record[1], record[2], 'recibidos', record[4], record[3]))
            
            # Procesar archivo enviados
            input_file_enviados = 'I:/VISION/A_THOM.txt'
            count_nvi, count_ink, count_hoya, count_semifinish, count_finished = process_enviados_file(input_file_enviados)
            cursor.execute(sql_insert_datos, (fecha_actual, count_nvi, count_ink, count_hoya, 'enviados', count_semifinish, count_finished))
            
            # Procesar archivo cancelados
            input_file_cancelados = 'I:/VISION/A_THO1.txt'
            count_nvi, count_ink, count_hoya = process_cancelados_file(input_file_cancelados)
            cursor.execute(sql_insert_datos, (fecha_actual, count_nvi, count_ink, count_hoya, 'cancelados', None, None))
            
            # Procesar razones de cancelados
            input_file_new = 'I:/VISION/A_THO20.txt'  # Cambia el nombre del archivo según sea necesario
            data_to_insert_new = process_new_file_with_translation(input_file_new)
            sql_insert_new_table = """
            INSERT INTO razones_cancelados (
                fecha, job_type, job_category, job_issue, job_count
            )
            VALUES (%s, %s, %s, %s, %s)
            """
            for record in data_to_insert_new:
                cursor.execute(sql_insert_new_table, (fecha_actual, record[0], record[1], record[2], record[3]))
            
            connection.commit()
            print("Datos insertados en las tablas.")
        except mysql.connector.Error as err:
            print("Error al ejecutar el comando SQL:", err)
        finally:
            if 'cursor' in locals() and cursor:
                cursor.close()
            if 'connection' in locals() and connection.is_connected():
                connection.close()
                print("Conexión cerrada.")
        print("Proceso completado.")