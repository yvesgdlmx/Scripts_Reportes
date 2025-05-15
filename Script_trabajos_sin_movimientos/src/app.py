import csv
import mysql.connector
from datetime import datetime, timedelta
# Configuración: cuando el valor es ambiguo, se utilizará este orden.
# 'mmddyy' significa que se interpretará el primer valor como mes y el segundo como día.
DEFAULT_FORMAT_AMBIGUOUS = 'mmddyy'
def robust_parse_date(date_str, filtro_diferencia_dias=None):
    """
    Intenta parsear la fecha usando los siguientes formatos:
      - "%m/%d/%Y", "%m/%d/%y", "%d/%m/%Y", "%d/%m/%y"
    Si se pasa el parámetro filtro_diferencia_dias (por ejemplo, 4) se devolverá el candidato cuya
    diferencia con la fecha actual sea la indicada.
    En caso ambiguo se usará la variable DEFAULT_FORMAT_AMBIGUOUS para decidir.
    """
    date_str = date_str.strip()
    posibles_formatos = ["%m/%d/%Y", "%m/%d/%y", "%d/%m/%Y", "%d/%m/%y"]
    candidatos = []
    for fmt in posibles_formatos:
        try:
            d = datetime.strptime(date_str, fmt).date()
            # Si el formato tiene año de 2 dígitos y es menor a 100, asumimos el siglo 2000.
            if fmt in ("%m/%d/%y", "%d/%m/%y") and d.year < 100:
                d = d.replace(year=d.year + 2000)
            candidatos.append((fmt, d))
        except ValueError:
            continue
    if not candidatos:
        raise ValueError(f"No se pudo parsear la fecha: {date_str}")
    # Si se requiere filtrar por diferencia (por ejemplo, para enter_date queremos 4 días de antigüedad).
    if filtro_diferencia_dias is not None:
        dia_actual = datetime.now().date()
        candidatos_validos = [d for fmt, d in candidatos if (dia_actual - d).days == filtro_diferencia_dias]
        if candidatos_validos:
            return candidatos_validos[0]
    # En caso ambiguo, recurrir a la configuración por defecto:
    # Para 'mmddyy' se elige el formato que empieza con "%m"
    if DEFAULT_FORMAT_AMBIGUOUS == 'mmddyy':
        for fmt, d in candidatos:
            if fmt.startswith("%m"):
                return d
    elif DEFAULT_FORMAT_AMBIGUOUS == 'ddmmyy':
        for fmt, d in candidatos:
            if fmt.startswith("%d"):
                return d
    # Si no coincide con la configuración o no es ambiguo, se devuelve el primer candidato.
    return candidatos[0][1]
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
            if len(row) < 11:
                print(f"Fila incompleta, se omite: {row}")
                continue
            try:
                # Se interpreta el campo enter_date con filtro de 4 días.
                enter_date_obj = robust_parse_date(row[0], filtro_diferencia_dias=4)
                enter_date = enter_date_obj.strftime('%Y-%m-%d')
                # Si la diferencia no es de 4 días, se omite el registro.
                if (dia_actual - enter_date_obj).days != 4:
                    continue
                acct = row[1].strip()
                tray_number = row[2].strip()
                ink_tray = row[3].strip()
                current_station = row[4].strip()
                
                # Para current_stn_date se usa robust_parse_date sin filtro (se asume por defecto).
                current_stn_date_obj = robust_parse_date(row[5])
                current_stn_date = current_stn_date_obj.strftime('%Y-%m-%d')
                division = row[6].strip()
                days_in_process = int(float(row[7].strip()))
                current_stn_time = parse_time_str(row[8].strip())
                coat = row[9].strip()
                f_s = row[10].strip()
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
        input_file = 'I:/VISION/a_IP.txt'
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