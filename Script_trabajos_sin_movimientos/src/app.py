import csv
import mysql.connector
from datetime import datetime, timedelta
def robust_parse_date(date_str, filtro_diferencia_dias=None):
    """
    Intenta parsear la fecha usando el siguiente orden de formatos, que se asume serán:
      - "%m/%d/%Y" y "%m/%d/%y"
    Es decir, se espera que la fecha venga en formato mm/dd/yy (o con año completo).
    
    Si se usa el formato de dos dígitos (mm/dd/yy) y el año es menor a 100,
    se asume que pertenece al siglo 2000.
    
    Si falla en parsear la fecha, se lanza ValueError.
    """
    date_str = date_str.strip()
    if not date_str:
        raise ValueError("Fecha vacía")
        
    formatos = ["%m/%d/%Y", "%m/%d/%y"]
    d = None
    for fmt in formatos:
        try:
            d = datetime.strptime(date_str, fmt).date()
            # Si se usó el formato de dos dígitos y el año es menor que 100, asumimos el siglo 2000.
            if fmt == "%m/%d/%y" and d.year < 100:
                d = d.replace(year=d.year + 2000)
            break
        except ValueError:
            continue
    if d is None:
        raise ValueError(f"No se pudo parsear la fecha: {date_str}")
    if filtro_diferencia_dias is not None:
        dia_actual = datetime.now().date()
        if (dia_actual - d).days != filtro_diferencia_dias:
            raise ValueError(f"La diferencia de días no es de {filtro_diferencia_dias}")
    return d
def parse_time_str(time_str):
    """
    Parsea una cadena de tiempo y devuelve un objeto time.
    Se esperan los formatos HH:MM o HH:MM:SS.
    """
    time_str = time_str.strip()
    formatos = ['%H:%M:%S', '%H:%M']
    for fmt in formatos:
        try:
            return datetime.strptime(time_str, fmt).time()
        except ValueError:
            continue
    raise ValueError(f"No se pudo parsear la hora: {time_str}")
def format_timedelta(td):
    total_seconds = int(td.total_seconds())
    horas = total_seconds // 3600
    minutos = (total_seconds % 3600) // 60
    segundos = total_seconds % 60
    return f"{horas}:{minutos:02}:{segundos:02}"
def process_file(input_file):
    registros = []  # Cada registro es una tupla.
    with open(input_file, 'r', encoding='utf-8', newline='') as file:
        reader = csv.reader(file, delimiter='\t')
        encabezados = next(reader)  # Se asume que la primera línea tiene los encabezados.
        now = datetime.now()
        dia_actual = now.date()
        for row in reader:
            # Se espera que la fila tenga 11 columnas.
            if len(row) < 11:
                print(f"Fila incompleta, se omite: {row}")
                continue
            try:
                # Parseamos enter_date usando únicamente el formato mm/dd/yy (o mm/dd/YYYY).
                enter_date_obj = robust_parse_date(row[0])
                enter_date = enter_date_obj.strftime('%Y-%m-%d')
                
                acct = row[1].strip()
                tray_number = row[2].strip()
                ink_tray = row[3].strip()
                current_station = row[4].strip()
                
                # Parseamos current_stn_date con el mismo formato.
                current_stn_date_obj = robust_parse_date(row[5])
                current_stn_date = current_stn_date_obj.strftime('%Y-%m-%d')
                
                division = row[6].strip()
                days_in_process = int(float(row[7].strip()))
                current_stn_time = parse_time_str(row[8].strip())
                coat = row[9].strip()
                f_s = row[10].strip()
                
                # Se calcula el tiempo transcurrido usando current_stn_date y current_stn_time.
                fecha_hora_origen = datetime.combine(current_stn_date_obj, current_stn_time)
                diferencia = now - fecha_hora_origen
                transcurrido = format_timedelta(diferencia)
                total_segundos = int(diferencia.total_seconds())
                registros.append((
                    enter_date,           # Fecha de entrada en formato ISO: YYYY-MM-DD
                    acct,
                    tray_number,
                    ink_tray,
                    current_station,
                    current_stn_date,     # Fecha de current station en formato ISO: YYYY-MM-DD
                    division,
                    days_in_process,
                    current_stn_time,
                    coat,
                    f_s,
                    dia_actual.strftime('%Y-%m-%d'),
                    now.time().replace(microsecond=0).isoformat(),
                    transcurrido,
                    total_segundos        # Campo auxiliar para ordenar (no se inserta)
                ))
            except Exception as e:
                print(f"Error al procesar la fila {row}: {e}")
                continue
    registros.sort(key=lambda x: x[14], reverse=True)
    return [registro[:14] for registro in registros]
def main():
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
        sql_truncate = "TRUNCATE TABLE trabajos_sin_movimientos"
        cursor.execute(sql_truncate)
        print("Tabla trabajos_sin_movimientos truncada exitosamente.")
        input_file = 'I:/VISION/a_IPEYE.txt'
        registros = process_file(input_file)
        sql_insert = """
        INSERT INTO trabajos_sin_movimientos (
            enter_date, acct, tray_number, ink_tray, current_station,
            current_stn_date, division, days_in_process, current_stn_time,
            coat, f_s, dia_actual, hora_actual, transcurrido
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(sql_insert, registros)
        connection.commit()
        print(f"Número de registros insertados: {cursor.rowcount}")
    except mysql.connector.Error as err:
        print("Error al ejecutar el comando SQL:", err)
    except Exception as ex:
        print("Ocurrió un error:", ex)
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()
            print("Conexión cerrada.")
    print("Proceso completado.")
if __name__ == '__main__':
    main()