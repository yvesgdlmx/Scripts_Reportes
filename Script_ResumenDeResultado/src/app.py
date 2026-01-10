import csv
import mysql.connector
from datetime import datetime, timedelta
import calendar

# Función para procesar el archivo enviados <-------------------------------------------------------------|
def process_enviados_file(input_file):
    # inicializar contadores
    total_semifinish = 0
    total_finished = 0

    # Abrir y leer el archivo
    with open(input_file, 'r') as file:
        # Crear lector CSV con delimitador de tabulación
        reader = csv.reader(file, delimiter='\t')
        # Saltar la primera línea (encabezados)
        next(reader, None)

        data_found = False
        for row in reader:
            if len(row) >= 3:
                data_found = True
                job_type = row[0] # Tipo de trabajo (NV, INK, HO)
                job_status = row[1] # Estado del trabajo (S=seminifinish, F=finished)
                try:
                    job_count = int(row[2]) # convertir la cantidad a entero
                except ValueError:
                    continue # si no se puede convertir ignorar ese registro
                # Clasificar los trabajos segun el tipo
                if job_type == 'NV':
                    if job_status == 'S': # Contar trabajos seminifinish (Status 'S')
                        total_semifinish += job_count
                    elif job_status == 'F': # Contar trabajos finished (Status 'F')
                        total_finished += job_count
                elif job_type == 'INK':
                    total_semifinish += job_count # INK siempre es semifinish, suma al total_semifinish
                elif job_type == 'HO':
                    total_semifinish += job_count # HO siempre es semifinish, suma al total_semifinish
    # Si no se encontraron datos validos, mostrar mensaje
    if not data_found:
        print("No se encontraron datos válidos en el archivo. insertando ceros.")
    # Retornar los totales que necesitamos para la tabla resumen_resultado
    return total_semifinish, total_finished

# Función para procesar el archivo net.txt y extraer la facturación <-------------------------------------------------------------|
def process_net_file(input_file):
    """
    Procesa el archivo net.txt para extraer el valor de facturación real.
    Busca la ÚLTIMA línea que contiene "GRAND TOTAL" y extrae el primer valor numérico.
    
    Args:
        input_file (str): Ruta del archivo net.txt
    
    Returns:
        float: Valor de facturación real (GRAND TOTAL)
    """
    facturacion_real = 0.0
    ultima_linea_grand_total = None
    
    # Abrir y leer el archivo para encontrar la última línea con GRAND TOTAL
    with open(input_file, 'r') as file:
        for line in file:
            # Guardar cada línea que contiene "GRAND TOTAL"
            if "GRAND TOTAL" in line:
                ultima_linea_grand_total = line
    
    # Procesar la última línea encontrada con GRAND TOTAL
    if ultima_linea_grand_total:
        # Dividir la línea por espacios en blanco
        parts = ultima_linea_grand_total.split()
        
        # Buscar "TOTAL" y obtener el siguiente valor numérico
        try:
            total_index = parts.index("TOTAL")
            # El primer valor numérico después de "TOTAL" es la facturación
            for i in range(total_index + 1, len(parts)):
                try:
                    facturacion_real = float(parts[i])
                    print(f"Facturación extraída de la línea: {ultima_linea_grand_total.strip()}")
                    print(f"Valor encontrado: {facturacion_real}")
                    break
                except ValueError:
                    continue
        except (ValueError, IndexError):
            print("No se pudo extraer el valor de facturación del GRAND TOTAL.")
    else:
        print("No se encontró ninguna línea con GRAND TOTAL en el archivo.")
    
    return facturacion_real   

# Función para verificar y crear registros del mes completo <-------------------------------------------------------------|
def verificar_y_crear_mes_completo(cursor, anio, mes):
    """
    Verifica si existen registros para todos los días del mes actual.
    Si faltan días, los inserta con valores NULL (solo semana y diario).
    INCLUYE LOS DOMINGOS en la inserción.
    
    Args:
        cursor: Cursor de la conexión MySQL
        anio (int): Año actual
        mes (int): Mes actual
    """
    # Calcular el número de días en el mes actual
    num_dias_mes = calendar.monthrange(anio, mes)[1]
    
    # Consultar qué fechas ya existen en la base de datos para este mes
    sql_verificar = """
    SELECT diario FROM resumen_resultados
    WHERE YEAR(diario) = %s AND MONTH(diario) = %s
    ORDER BY diario
    """
    cursor.execute(sql_verificar, (anio, mes))
    fechas_existentes = cursor.fetchall()
    
    # Convertir las fechas existentes a un conjunto para búsqueda rápida
    fechas_existentes_set = {fecha[0] for fecha in fechas_existentes}
    
    print(f"Verificando mes {mes}/{anio}...")
    print(f"Días en el mes: {num_dias_mes}")
    print(f"Registros existentes: {len(fechas_existentes_set)}")
    
    # Generar todas las fechas del mes (INCLUYENDO DOMINGOS)
    fechas_a_insertar = []
    for dia in range(1, num_dias_mes + 1):
        fecha = datetime(anio, mes, dia).date()
        
        # Si la fecha no existe, agregarla a la lista de inserciones
        if fecha not in fechas_existentes_set:
            # Calcular el número de semana para esta fecha
            semana = datetime(anio, mes, dia).isocalendar()[1]
            fechas_a_insertar.append((semana, fecha))
            
            # Mostrar si es domingo (solo informativo)
            if fecha.isoweekday() == 7:
                print(f"Insertando {fecha} (domingo)")
    
    # Insertar las fechas faltantes con valores NULL
    if fechas_a_insertar:
        print(f"Insertando {len(fechas_a_insertar)} días faltantes...")
        sql_insert_vacio = """
        INSERT INTO resumen_resultados (semana, diario)
        VALUES (%s, %s)
        """
        cursor.executemany(sql_insert_vacio, fechas_a_insertar)
        print(f"✓ {len(fechas_a_insertar)} registros insertados con valores NULL")
    else:
        print("✓ Todos los días del mes ya están registrados")

def process_turnos_file(input_file):
    """
    Procesa el archivo de facturación por turnos y categoriza los trabajos según la hora.
    
    Turnos:
    - Matutino: 21:30 - 14:29 (incluye noche y mañana)
    - Vespertino: 14:30 - 21:29
    
    Args:
        input_file (str): Ruta del archivo de facturación
    
    Returns:
        tuple: (trabajos_matutino, trabajos_vespertino)
    """
    trabajos_matutino = 0
    trabajos_vespertino = 0

    try: 
        with open(input_file, 'r') as file:
            # Crear lector csv con delimitador de tabulacion
            reader = csv.reader(file, delimiter='\t')
            # Saltar la primera linea (encabezado)
            next(reader, None)

            for row in reader:
                if len(row) >= 4:
                    try:
                        # La hora esta en la columna 3
                        hora_str = row[3].strip()
                        # Convertir hora a objeto time para comparacion
                        hora_obj = datetime.strptime(hora_str, '%H:%M').time()
                        # Convertir hora a minutos totales para facilitar la comparacion
                        # Esto facilita el manejo de rangos que cruzan la medianoche
                        hora_minutos = hora_obj.hour * 60 + hora_obj.minute
                        # Defiinir los rangos de turnos en minutos:
                        # 21:30 =  1290 minutos
                        # 14:29 = 869 minutos
                        # 14:30 = 870 minutos
                        # 21:29 = 1289 minutos
                        if (hora_minutos >= 1290) or (hora_minutos <= 869):  # <<<< CORREGIDO: era "jora_minutos"
                            # Turno matutino 21:30-23:59 0 00:00-14:29
                            trabajos_matutino += 1
                        elif 870 <= hora_minutos <= 1289:
                            # Turno vespertino 14:30-21:29
                            trabajos_vespertino += 1 
                    except (ValueError, IndexError) as e:
                        # Si hay un error al convertir la hora, ignorar ese registro
                        continue
        print(f"Trabajos procesados - Matutino: {trabajos_matutino}, Vespertino: {trabajos_vespertino}")
    except FileNotFoundError:
        print(f"No se encontro el archivo: {input_file}")
        return 0, 0
    except Exception as e:
        print(f"Error al procesar el archivo de turnos: {e}")
        return 0, 0
    
    return trabajos_matutino, trabajos_vespertino  # <<<< AGREGADO: retornar la tupla

# Función para actualizar o insertar el registro del día actual <-------------------------------------------------------------|
def actualizar_o_insertar_dia_actual(cursor, semana_actual, fecha_actual, real_sf, real_f, real_suma, facturacion_real, trabajos_mat, trabajos_vesp):  # <<<< CORREGIDO: agregados parámetros
    """
    Verifica si ya existe un registro para la fecha actual.
    Si existe, lo actualiza. Si no existe, lo inserta.
    
    Args:
        cursor: Cursor de la conexión MySQL
        semana_actual (int): Número de semana actual
        fecha_actual (str): Fecha actual en formato YYYY-MM-DD
        real_sf (int): Total de trabajos semifinished
        real_f (int): Total de trabajos finished
        real_suma (int): Suma total de trabajos
        facturacion_real (float): Valor de facturación real
        trabajos_mat (int): Total de trabajos turno matutino
        trabajos_vesp (int): Total de trabajos turno vespertino
    """
    # Verificar si ya existe un registro para hoy
    sql_verificar_hoy = "SELECT id FROM resumen_resultados WHERE diario = %s"
    cursor.execute(sql_verificar_hoy, (fecha_actual,))
    registro_existe = cursor.fetchone()
    
    if registro_existe:
        # Si existe, actualizar el registro
        print(f"Registro encontrado para {fecha_actual}. Actualizando...")
        sql_update = """
        UPDATE resumen_resultados
        SET real_sf = %s, real_f = %s, real_suma = %s, facturacion_real = %s,
            trabajos_mat = %s, trabajos_vesp = %s
        WHERE diario = %s
        """
        cursor.execute(sql_update, (real_sf, real_f, real_suma, facturacion_real, trabajos_mat, trabajos_vesp, fecha_actual))  # <<<< CORREGIDO: agregados parámetros
        print(f"✓ Registro actualizado para la fecha {fecha_actual}")
    else:
        # Si no existe, insertar un nuevo registro
        print(f"No existe registro para {fecha_actual}. Insertando...")
        sql_insert = """
        INSERT INTO resumen_resultados (
            semana, diario, real_sf, real_f, real_suma, facturacion_real, 
            trabajos_mat, trabajos_vesp
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(sql_insert, (semana_actual, fecha_actual, real_sf, real_f, real_suma, facturacion_real, trabajos_mat, trabajos_vesp))
        print(f"✓ Registro insertado para la fecha {fecha_actual}")

# Punto de entrada principal del script <-------------------------------------------------------------|
if __name__ == "__main__":
    # Verificar que no sea domingo (dia 7 de la semana)
    # Si es domingo, no ejecutar el script (pero sí se habrán insertado los domingos en el paso 1)
    if datetime.now().isoweekday() == 7:
        print("Hoy es domingo. El script no se ejecutará.")
    else:
        try:
            # Establecer conexión con la base de datos MYSQL
            connection = mysql.connector.connect(
                host='autorack.proxy.rlwy.net',
                port=22723,
                user='root',
                password='zsulNCCrYFSfBqIxwwIXIKqLQKFJWwbw',
                database='railway'
            )

            #Verificar si la conexión fue exitosa
            if connection.is_connected():
                print("Conexión a la base de datos exitosa.")
            
            cursor = connection.cursor() # Crear cursor para ejecutar consultas SQL
            
            # Obtener datos de fecha actual
            fecha_actual_obj = datetime.now()
            fecha_actual = fecha_actual_obj.strftime('%Y-%m-%d') # Formato YYYY-MM-DD
            semana_actual = fecha_actual_obj.isocalendar()[1] # Número de semana
            anio_actual = fecha_actual_obj.year
            mes_actual = fecha_actual_obj.month
            
            print("="*70)
            print(f"INICIANDO PROCESO PARA: {fecha_actual} (Semana {semana_actual})")
            print("="*70)
            
            # PASO 1: Verificar y crear todos los días del mes actual (INCLUYENDO DOMINGOS)
            print("\n[PASO 1] Verificando días del mes completo...")
            verificar_y_crear_mes_completo(cursor, anio_actual, mes_actual)
            
            # PASO 2: Procesar archivos y obtener datos del día actual
            print("\n[PASO 2] Procesando archivos del día actual...")
            
            # Procesar el archivo enviados
            input_file_enviados = 'I:/VISION/A_THOM.txt'
            real_sf, real_f = process_enviados_file(input_file_enviados)
            real_suma = real_sf + real_f
            
            # Procesar el archivo net.txt para obtener la facturacion real
            input_file_net = 'I:/VISION/net.txt'
            facturacion_real = process_net_file(input_file_net)

            # Procesar el archivo de turnos para obtener trabajos por turno.
            input_file_turnos = 'I:/VISION/A_SPALL.txt'
            trabajos_mat, trabajos_vesp = process_turnos_file(input_file_turnos)
            
            # PASO 3: Actualizar o insertar el registro del día actual
            print("\n[PASO 3] Actualizando registro del día actual...")
            actualizar_o_insertar_dia_actual(
                cursor, 
                semana_actual, 
                fecha_actual, 
                real_sf, 
                real_f, 
                real_suma, 
                facturacion_real,
                trabajos_mat,      # <<<< CORREGIDO: agregado parámetro
                trabajos_vesp      # <<<< CORREGIDO: agregado parámetro
            )
            
            # Confirmar todos los cambios en la base de datos
            connection.commit()
            
            # Resumen final
            print("\n" + "="*70)
            print("RESUMEN DEL PROCESO")
            print("="*70)
            print(f"Fecha procesada: {fecha_actual}")
            print(f"Semana: {semana_actual}")
            print(f"Real SF (Semifinished): {real_sf}")
            print(f"Real F (Finished): {real_f}")
            print(f"Real Suma (Total): {real_suma}")
            print(f"Facturación Real: ${facturacion_real:,.2f}")
            print(f"Trabajos Matutino: {trabajos_mat}")      # <<<< CORREGIDO: agregado al resumen
            print(f"Trabajos Vespertino: {trabajos_vesp}")   # <<<< CORREGIDO: agregado al resumen
            print("="*70)

        except mysql.connector.Error as err:
            print(f"❌ Error al conectar a la base de datos: {err}")
        except FileNotFoundError as err:
            print(f"❌ Error: No se encontró el archivo: {err}")
        except Exception as err:
            print(f"❌ Error inesperado: {err}")
        finally:
            # Cerrar el cursor si existe
            if 'cursor' in locals() and cursor:
                cursor.close()
            # Cerrar la conexión si esta abierta
            if 'connection' in locals() and connection.is_connected():
                connection.close()
                print("\n✓ Conexión a la base de datos cerrada.")
        
        print("\n✓ Script finalizado.")