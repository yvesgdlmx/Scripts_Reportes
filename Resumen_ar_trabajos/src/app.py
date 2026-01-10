import csv
import mysql.connector
from datetime import datetime

def get_rounded_time(current_time):
    """Redondea la hora según el turno"""
    hour = current_time.hour
    minute = current_time.minute
    
    # Turno diurno: 06:30 - 21:59 (SIEMPRE redondear hacia :30)
    if 6 <= hour <= 21 and not (hour == 6 and minute < 30):
        if minute >= 30:
            rounded_minute = 30
        else:
            # Si es antes de :30, redondear a :30 de la MISMA hora
            rounded_minute = 30
        return f"{hour:02d}:{rounded_minute:02d}:00"
    
    # Turno nocturno: 22:00 - 06:29 (hora cerrada :00)
    else:
        return f"{hour:02d}:00:00"

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

def categorize_station(estacion):
    """Categoriza la estación según los criterios especificados"""
    # Extraer el primer elemento del nombre de la estación
    estacion_parts = estacion.split()
    if not estacion_parts:
        return None
    
    first_part = estacion_parts[0]
    
    # Debug temporal - quitar después
    print(f"Debug - Estación: '{estacion}' -> First part: '{first_part}'")
    
    # EN AR
    en_ar_stations = {"52", "53", "54", "55", "56", "46", "48", "49", "50", "91", "92", "60", "OTB", "44", "66"}
    if first_part in en_ar_stations:
        print(f"Debug - '{first_part}' encontrado en en_ar_stations")
        return "en_ar"
    
    # ANTES DE AR
    antes_de_ar_stations = {"19", "20", "219", "220", "221", "223", "224", "225", "226", "241", "242", 
                           "250", "245", "246", "244", "243", "247", "248", "255", "256", "257", 
                           "258", "259", "260", "261", "262", "265", "266", "267", "268", "269", 
                           "254", "270", "271", "272", "273", "400", "222", "80", "85", "335", "249"}
    if first_part in antes_de_ar_stations:
        print(f"Debug - '{first_part}' encontrado en antes_de_ar_stations")
        return "antes_de_ar"
    
    # SIN SURTIR
    sin_surtir_stations = {"135", "140", "147", "152", "148", "137", "151", "154", "134", "136", "141", "155", "169", "170"}
    if first_part in sin_surtir_stations:
        print(f"Debug - '{first_part}' encontrado en sin_surtir_stations")
        return "sin_surtir"
    
    # RECALCULANDO
    recalculando_stations = {"10", "04", "150"}
    if first_part in recalculando_stations:
        print(f"Debug - '{first_part}' encontrado en recalculando_stations")
        return "recalculando"
    
    # BREAKAGE
    if first_part == "BREAKAGE":
        print(f"Debug - '{first_part}' es BREAKAGE")
        return "breakage"
    
    print(f"Debug - '{first_part}' NO encontrado en ninguna categoría")
    return None

def process_prueba_file(filename):
    """Procesa el archivo A_PRUEBA.txt y agrupa por fecha y categoría"""
    station_data = {}
    
    print(f"Procesando archivo: {filename}")
    
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            reader = csv.reader(file, delimiter='\t')
            # Saltar la primera línea (encabezados)
            next(reader)
            
            for row in reader:
                if len(row) >= 13:
                    fecha = row[0].strip()
                    nvi_arsemi = row[7].strip()  # Columna NVI ARSemi (índice 7)
                    estacion = row[12].strip()
                    
                    # Solo procesar si NVI ARSemi = 1
                    if nvi_arsemi == '1':
                        fecha_parsed = parse_date(fecha)
                        if fecha_parsed:
                            categoria = categorize_station(estacion)
                            if fecha_parsed not in station_data:
                                station_data[fecha_parsed] = {
                                    'en_ar': 0,
                                    'antes_de_ar': 0,
                                    'sin_surtir': 0,
                                    'recalculando': 0,
                                    'breakage': 0,
                                    'despues_de_ar': 0
                                }
                            
                            if categoria:
                                station_data[fecha_parsed][categoria] += 1
                            else:
                                # Contar estaciones no categorizadas como "después de AR"
                                station_data[fecha_parsed]['despues_de_ar'] += 1
                                print(f"Estación no categorizada (después de AR): {estacion}")
    
    except Exception as e:
        print(f"Error procesando archivo: {str(e)}")
    
    return station_data

try:
    # Conexión a la base de datos
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
    
    # Procesar archivo A_ARVIC.txt
    prueba_file = 'I:/VISION/A_ARVIC.txt'
    station_data = process_prueba_file(prueba_file)
    
    # Obtener fecha y hora actuales
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
    
    print(f"Turno actual: {turno}")
    print(f"Fecha de inserción: {current_date}")
    print(f"Hora de inserción: {hora_insercion}")
    
    if not skip_insertion:
        # Preparar datos para inserción
        if station_data:
            sql_insert = """
            INSERT INTO resumen_ar_nvis
            (fecha, en_ar, antes_de_ar, sin_surtir, recalculando, breakage, despues_de_ar, fecha_insercion, hora_insercion)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                en_ar = VALUES(en_ar),
                antes_de_ar = VALUES(antes_de_ar),
                sin_surtir = VALUES(sin_surtir),
                recalculando = VALUES(recalculando),
                breakage = VALUES(breakage),
                despues_de_ar = VALUES(despues_de_ar),
                fecha_insercion = VALUES(fecha_insercion),
                hora_insercion = VALUES(hora_insercion)
            """
            
            data_to_insert = [
                (fecha, data['en_ar'], data['antes_de_ar'], data['sin_surtir'], 
                 data['recalculando'], data['breakage'], data['despues_de_ar'], current_date, hora_insercion)
                for fecha, data in station_data.items()
            ]
            
            cursor.executemany(sql_insert, data_to_insert)
            print(f"Se procesaron {len(data_to_insert)} registros")
            
            # Mostrar resumen de datos procesados
            for fecha, data in station_data.items():
                print(f"Fecha: {fecha} - En AR: {data['en_ar']}, Antes de AR: {data['antes_de_ar']}, "
                      f"Sin Surtir: {data['sin_surtir']}, Recalculando: {data['recalculando']}, "
                      f"Breakage: {data['breakage']}, Después de AR: {data['despues_de_ar']}")
            
            connection.commit()
        else:
            print("No se encontraron datos para procesar.")
    else:
        print("Se evitó la inserción de datos debido a la validación del turno.")

except mysql.connector.Error as error:
    print(f"Error con la base de datos: {error}")

finally:
    if 'connection' in locals() and connection.is_connected():
        cursor.close()
        connection.close()
        print("\nConexión cerrada")