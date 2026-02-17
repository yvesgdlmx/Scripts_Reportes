import csv
import mysql.connector
from datetime import datetime
def get_rounded_time(current_time):
    hour = current_time.hour
    minute = current_time.minute
    if minute >= 30:
        rounded_minute = 30
    else:
        rounded_minute = 0
        if minute < 30 and hour > 0:
            hour = hour - 1
            rounded_minute = 30
    return f"{hour:02d}:{rounded_minute:02d}:00"
def parse_date(date_str):
    date_formats = [
        '%m/%d/%Y',    # 01/13/2025
        '%m/%d/%y',    # 01/13/25
        '%d/%m/%Y',    # 15/01/2025
        '%d/%m/%y',    # 15/01/25
    ]
    for fmt in date_formats:
        try:
            date = datetime.strptime(date_str, fmt)
            if date.year < 100:
                date = date.replace(year = date.year + 2000)
            return date.strftime('%Y-%m-%d')
        except ValueError:
            continue
    print(f"No se pudo parsear la fecha: {date_str}")
    return None
def process_stations_file(filename):
    # Lista de estaciones permitidas
    estaciones_permitidas = {
        "04 DIGITAL CALC", "10 RX ENTRY", "134 Q-NVI B50 AR", "135 Q-NVI B50", "136 Q-NVI P F",
        "137 Q-NVI PLY F", "138 Q-NVI B F AR", "139 Q-NVI BLY F", "140 Q-NVI BLY", "141 Q-NVI BLY AR",
        "142 Q-NVI BLY TR", "143 Q-NVI BLY AT", "144 Q-NVI CR A T", "145 Q-NVI PY AT", "146 Q-NVI PLY TR",
        "147 Q-NVI CR39", "148 Q-NVI PLY AR", "149 Q-NVI TRACE", "150 Q-DIG CALC", "151 Q-CALC FAIL",
        "152 Q-NVI JOBS", "153 Q-NVI NO ES", "154 Q-NVI AR", "155 Q-NVI NO QOH", "Q-HOYA JOBS",
        "Q-HOYA BAD PICK", "Q-LENS ISSUE", "Q-INK", "Q-HIPWR", "Q-HOYA NO QOH", "Q-INK NO QOH",
        "Q-JAI KUDO JOBS", "OPTIMEX", "INK LIFT OPTICS", "166 POLY AR F", "167 CR AR F", "168 CR 75", "169 AR FRAME P",
    }
    station_counts = {}
    # Se crearán dos diccionarios para acumular los valores NO surtidos de ARSemi y SinAR Semi (solo para registros con tipo S)
    sums_ar_semi = {}
    sums_sin_ar_semi = {}
    print(f"Procesando archivo de estaciones: {filename}")
    try:
        with open(filename, 'r') as file:
            reader = csv.reader(file, delimiter='\t')
            next(reader)
            for row in reader:
                if len(row) >= 13:
                    fecha = row[0]
                    tipo = row[2].strip()
                    estacion = row[12].strip()
                    fecha_parsed = parse_date(fecha)
                    # Verificamos si la estación está en la lista de permitidas y si el registro es de tipo F o S
                    if fecha_parsed and tipo in ['F', 'S'] and estacion in estaciones_permitidas:
                        key = (fecha_parsed, tipo)
                        station_counts[key] = station_counts.get(key, 0) + 1
                        # Para registros de tipo S, acumular los valores de NO surtido correspondientes a ARSemi y SinAR Semi
                        if tipo == 'S':
                            try:
                                # Se asume el siguiente orden en las columnas:
                                # 7: NVI ARSemi
                                # 10: NVI SinAR Semi
                                ar_semi_val = int(float(row[7].strip()))
                                sin_ar_semi_val = int(float(row[10].strip()))
                            except Exception as ex:
                                print(f"Error parseando valores ARSemi en fila: {row} - {ex}")
                                ar_semi_val = 0
                                sin_ar_semi_val = 0
                            sums_ar_semi[fecha_parsed] = sums_ar_semi.get(fecha_parsed, 0) + ar_semi_val
                            sums_sin_ar_semi[fecha_parsed] = sums_sin_ar_semi.get(fecha_parsed, 0) + sin_ar_semi_val
    except Exception as e:
        print(f"Error procesando archivo de estaciones: {str(e)}")
    return station_counts, sums_ar_semi, sums_sin_ar_semi
def process_summary_file(filename, station_counts):
    summary_data = []
    print(f"Procesando archivo de resumen: {filename}")
    try:
        with open(filename, 'r') as file:
            reader = csv.reader(file, delimiter='\t')
            next(reader)
            for row in reader:
                if len(row) >= 11:
                    fecha = parse_date(row[0])
                    if fecha:
                        nvi_fs = int(float(row[2].strip())) if row[2].strip() else 0
                        nvi_total_term = int(float(row[3].strip()))
                        nvi_total_ster = int(float(row[4].strip()))
                        # Obtener valores de no_surtido_term y no_surtido_ster del archivo de estaciones
                        no_surtido_term = station_counts.get((fecha, 'F'), 0)
                        no_surtido_ster = station_counts.get((fecha, 'S'), 0)
                        # Calcular los valores de surtido_term y surtido_ster
                        surtido_term = nvi_total_term - no_surtido_term
                        surtido_ster = nvi_total_ster - no_surtido_ster
                        summary_data.append({
                            'fecha': fecha,
                            'nvi_en_proceso': int(float(row[1].strip())),
                            'nvi_fs': nvi_fs,
                            'nvi_total_term': nvi_total_term,
                            'nvi_total_ster': nvi_total_ster,
                            'no_surtido_term': no_surtido_term,
                            'no_surtido_ster': no_surtido_ster,
                            'surtido_term': surtido_term,
                            'surtido_ster': surtido_ster,
                            'nvi_con_ar': int(float(row[5].strip())),
                            'nvi_ar_term': int(float(row[6].strip())),
                            'nvi_ar_semi': int(float(row[7].strip())),
                            'nvi_sin_ar': int(float(row[8].strip())),
                            'nvi_sin_ar_term': int(float(row[9].strip())),
                            'nvi_sin_ar_semi': int(float(row[10].strip()))
                        })
    except Exception as e:
        print(f"Error procesando archivo de resumen: {str(e)}")
    return summary_data
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
    # Procesar archivo de estaciones (A_INARFD.txt)
    stations_file = 'I:/VISION/A_INARFD.txt'
    station_counts, sums_ar_semi, sums_sin_ar_semi = process_stations_file(stations_file)
    # Procesar archivo de resumen (A_INARF1.txt)
    summary_file = 'I:/VISION/A_INARF1.txt'
    summary_data = process_summary_file(summary_file, station_counts)
    now = datetime.now()
    current_date = now.strftime('%Y-%m-%d')
    # Determinar el turno según la hora actual
    diurno_inicio = datetime.strptime("06:30", "%H:%M").time()
    diurno_fin = datetime.strptime("22:00", "%H:%M").time()
    current_time = now.time()
    turno = "diurno" if diurno_inicio <= current_time < diurno_fin else "nocturno"
    # Condicionales para evitar inserciones en ciertos rangos de minutos
    skip_insertion = False
    if turno == "diurno" and 0 <= now.minute <= 10:
        print("Turno diurno: No se insertarán datos si se ejecuta entre los minutos 00 y 10.")
        skip_insertion = True
    elif turno == "nocturno" and 30 <= now.minute <= 40:
        print("Turno nocturno: No se insertarán datos si se ejecuta entre los minutos 30 y 40.")
        skip_insertion = True
    # Definir hora de inserción según el turno
    hora_insercion = get_rounded_time(now) if turno == "diurno" else now.strftime("%H:00:00")
    if not skip_insertion:
        sql_insert_summary = """
        INSERT INTO resumen_nvis
        (fecha, nvi_en_proceso, nvi_fs, nvi_total_term, nvi_total_ster, 
         no_surtido_term, no_surtido_ster, surtido_term, surtido_ster,
         nvi_con_ar, nvi_ar_term, nvi_ar_semi, nvi_sin_ar, nvi_sin_ar_term, nvi_sin_ar_semi,
         no_surtido_ar_semi, no_surtido_sin_ar_semi, fecha_insercion, hora_insercion)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            nvi_en_proceso = VALUES(nvi_en_proceso),
            nvi_fs = VALUES(nvi_fs),
            nvi_total_term = VALUES(nvi_total_term),
            nvi_total_ster = VALUES(nvi_total_ster),
            no_surtido_term = VALUES(no_surtido_term),
            no_surtido_ster = VALUES(no_surtido_ster),
            surtido_term = VALUES(surtido_term),
            surtido_ster = VALUES(surtido_ster),
            nvi_con_ar = VALUES(nvi_con_ar),
            nvi_ar_term = VALUES(nvi_ar_term),
            nvi_ar_semi = VALUES(nvi_ar_semi),
            nvi_sin_ar = VALUES(nvi_sin_ar),
            nvi_sin_ar_term = VALUES(nvi_sin_ar_term),
            nvi_sin_ar_semi = VALUES(nvi_sin_ar_semi),
            no_surtido_ar_semi = VALUES(no_surtido_ar_semi),
            no_surtido_sin_ar_semi = VALUES(no_surtido_sin_ar_semi),
            fecha_insercion = VALUES(fecha_insercion),
            hora_insercion = VALUES(hora_insercion)
        """
        data_to_insert_summary = [
            (item['fecha'], item['nvi_en_proceso'], item['nvi_fs'], item['nvi_total_term'], item['nvi_total_ster'], 
             item['no_surtido_term'], item['no_surtido_ster'], item['surtido_term'], item['surtido_ster'],
             item['nvi_con_ar'], item['nvi_ar_term'], item['nvi_ar_semi'], item['nvi_sin_ar'], item['nvi_sin_ar_term'], 
             item['nvi_sin_ar_semi'], 
             sums_ar_semi.get(item['fecha'], 0), sums_sin_ar_semi.get(item['fecha'], 0),
             current_date, hora_insercion)
            for item in summary_data
        ]
        if data_to_insert_summary:
            cursor.executemany(sql_insert_summary, data_to_insert_summary)
            print(f"Se procesaron {len(data_to_insert_summary)} registros de resumen")
        connection.commit()
    else:
        print("Se evitó la inserción de datos debido a la validación del turno.")
except mysql.connector.Error as error:
    print(f"Error con la base de datos: {error}")
finally:
    if 'connection' in locals() and connection.is_connected():
        cursor.close()
        connection.close()
        print("\nConexión cerrada")