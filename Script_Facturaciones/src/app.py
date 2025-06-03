import csv
import mysql.connector
from datetime import datetime, timedelta
# ===============================================
# PROCESAMIENTO DEL PRIMER ARCHIVO (facturacion_nvi) 
# ===============================================
def process_file(input_file):
    """
    Procesa el archivo, ignorando la primera columna de cada fila y convirtiendo
    los siguientes 26 valores a sus respectivos tipos:
      - Los valores en posiciones pares se convierten a INT.
      - Los valores en posiciones impares se convierten a float.
    Se omiten filas que no tengan al menos 27 columnas.
    """
    registros = []
    with open(input_file, 'r', encoding='utf-8', newline='') as file:
        reader = csv.reader(file, delimiter='\t')
        encabezados = next(reader)  # se asume que hay encabezados
        for row in reader:
            if len(row) < 27:
                print("Fila incompleta, se omite:", row)
                continue
            try:
                # Se ignora la primera columna y se toman las siguientes 26.
                datos = row[1:27]
                valores = []
                for idx, val in enumerate(datos):
                    val = val.strip()
                    if idx % 2 == 0:
                        valores.append(int(float(val)))
                    else:
                        valores.append(float(val))
                registros.append(tuple(valores))
            except Exception as e:
                print("Error procesando la fila:", row, e)
    return registros
def extraer_total_real(archivo):
    """
    Extrae el valor total real del archivo proporcionado.
    Busca la primera línea que contiene '** TOTAL  RX **' y obtiene el último valor de la fila siguiente.
    """
    total_real = None
    with open(archivo, 'r', encoding='utf-8') as file:
        lines = file.readlines()  # Leer todas las líneas del archivo
        for i, line in enumerate(lines):
            # Usamos strip() para eliminar espacios adicionales y asegurarnos de que la comparación sea correcta
            if '** TOTAL  RX' in line.strip():  # Comparamos sin espacios
                # Leer la siguiente línea
                if i + 1 < len(lines):  # Asegurarse de que hay una línea siguiente
                    siguiente_linea = lines[i + 1].strip()
                    # Extraer el último valor de la línea
                    valores = siguiente_linea.split()
                    if valores:  # Verificamos que la línea no esté vacía
                        total_real = valores[-1]  # El último valor de la línea
                break  # Salir del bucle después de encontrar el primer total
    return total_real
def main_nvi():
    input_file = 'I:/VISION/A_KMNVI.txt'  # Actualiza la ruta según corresponda.
    registros = process_file(input_file)
    if not registros:
        print("No se encontraron registros para insertar en facturacion_nvi.")
        return
    hoy = datetime.now()
    fecha_str = hoy.strftime('%Y-%m-%d')
    semana = hoy.isocalendar()[1]  # número de semana en formato entero
    nuevos_registros = [(fecha_str, semana) + registro for registro in registros]
    try:
        conexion = mysql.connector.connect(
            host='autorack.proxy.rlwy.net',
            port=22723,
            user='root',
            password='zsulNCCrYFSfBqIxwwIXIKqLQKFJWwbw',
            database='railway'
        )
        cursor = conexion.cursor()
        sql_insert = """
        INSERT INTO facturacion_nvis (
            fecha,
            semana,
            cot_lenses,
            cot_coat,
            surf_lenses,
            surf_cost,
            ar_lenses,
            ar,
            p_frm_s_lenses,
            p_frm_s,
            p_frm_f_lenses,
            p_frm_f,
            m_frm_s_lenses,
            m_frm_s,
            m_frm_f_lenses,
            m_frm_f,
            grad_s_lenses,
            grad_s,
            grad_f_lenses,
            grad_f,
            sol_s_lenses,
            sol_s,
            sol_f_lenses,
            sol_f,
            uv_s_lenses,
            uv_s,
            uv_f_lenses,
            uv_f,
            total_real
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, NULL  -- o 0
        )
        """
        cursor.executemany(sql_insert, nuevos_registros)
        conexion.commit()
        print("Número de registros insertados en facturacion_nvis:", cursor.rowcount)
        # Ahora extraemos el total real del segundo archivo
        archivo_total = 'I:/VISION/net.txt'  # Cambia esta ruta al archivo correcto
        total_real = extraer_total_real(archivo_total)
        if total_real is not None:
            print("Total real extraído:", total_real)
            # Actualizar la columna total_real para el último registro insertado
            sql_update = "UPDATE facturacion_nvis SET total_real = %s WHERE fecha = %s AND semana = %s"
            cursor.execute(sql_update, (total_real, fecha_str, semana))
            conexion.commit()
            print("Total real actualizado en facturacion_nvis.")
        else:
            print("No se encontró el total real en el archivo.")
    except mysql.connector.Error as err:
        print("Error en MySQL (facturacion_nvi):", err)
    except Exception as ex:
        print("Error:", ex)
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conexion' in locals() and conexion.is_connected():
            conexion.close()
# ===================================================
# PROCESAMIENTO DEL SEGUNDO ARCHIVO (acumulación facturacion_hoya)
# ===================================================
def contar_trabajos_tallados(input_file):
    """
    Abre el archivo, omite la cabecera y retorna el número total de filas restantes.
    Cada fila se considera como 1 trabajo tallado.
    """
    with open(input_file, 'r', encoding='utf-8', newline='') as file:
        reader = csv.reader(file, delimiter='\t')
        next(reader, None)  # Omitir la cabecera
        total_filas = sum(1 for row in reader if row)
    return total_filas
def sumar_precio_tallado(input_file):
    """
    Recorre el archivo, omite la cabecera y suma el "precio tallado" de cada fila.
    Para cada fila se toman los valores de las columnas:
      ST35 TTL, ST28 TTL, STT7X28 TTL, STT8X35, SV y SV POLAR POLY TTL.
    Se asume que en la porción row[1:25] estos valores están en los índices:
      0 --> ST35 TTL
      4 --> ST28 TTL
      8 --> STT7X28 TTL
      12 -> STT8X35
      16 -> SV
      20 -> SV POLAR POLY TTL
    Cada valor se multiplica por 3 (1*3 = 3, 2*3 = 6, 0*3 = 0) y se suman.
    """
    suma_dolares = 0.0
    with open(input_file, 'r', encoding='utf-8', newline='') as file:
        reader = csv.reader(file, delimiter='\t')
        next(reader, None)  # Omitir la cabecera
        for row in reader:
            if len(row) < 25:
                print("Fila incompleta, se omite:", row)
                continue
            datos = row[1:25]
            # Índices de las columnas relevantes
            indices = [0, 4, 8, 12, 16, 20]
            for idx in indices:
                try:
                    valor = int(datos[idx].strip())
                except Exception:
                    valor = 0
                suma_dolares += valor * 3
    return suma_dolares
def contar_trabajos_hc(input_file):
    """
    Recorre el archivo, omite la cabecera y suma el "trabajos_hc" de cada fila.
    Se toman los valores de las columnas:
      ST35 W COT, ST28 W COT, 7X28 W COT, 8X35 W COT, SV W COT y SV POLAR W COT.
    Dado que en cada una esos valores pueden ser 0, 1 o 2, se contará 1 trabajo si el valor es 1 o 2,
    y 0 si es 0.
    La función retorna la suma total a lo largo de todas las filas.
    """
    total_trabajos_hc = 0
    # Índices de las columnas relevantes en row[1:25]
    indices = [3, 7, 11, 15, 19, 23]
    with open(input_file, 'r', encoding='utf-8', newline='') as file:
        reader = csv.reader(file, delimiter='\t')
        next(reader, None)  # Omitir la cabecera
        for row in reader:
            if len(row) < 25:
                print("Fila incompleta, se omite:", row)
                continue
            datos = row[1:25]
            for idx in indices:
                try:
                    valor = int(datos[idx].strip())
                except Exception:
                    valor = 0
                if valor in (1, 2):
                    total_trabajos_hc += 1
    return total_trabajos_hc
def sumar_precio_hc(input_file):
    """
    Recorre el archivo, omite la cabecera y suma el "precio hc" de cada fila.
    Para cada fila se toman los valores de las columnas:
      ST35 W COT, ST28 W COT, 7X28 W COT, 8X35 W COT, SV W COT y SV POLAR W COT.
    Se asume que en la porción row[1:25] estos valores se encuentran en los índices:
      3  -> ST35 W COT
      7  -> ST28 W COT
      11 -> 7X28 W COT
      15 -> 8X35 W COT
      19 -> SV W COT
      23 -> SV POLAR W COT
    Si el valor es 1 se suma 0.625, si es 2 se suma 1.25, y si es 0 se suma 0.
    La función retorna la suma total de "precio hc".
    """
    suma_hc = 0.0
    indices = [3, 7, 11, 15, 19, 23]
    with open(input_file, 'r', encoding='utf-8', newline='') as file:
        reader = csv.reader(file, delimiter='\t')
        next(reader, None)  # Omitir la cabecera
        for row in reader:
            if len(row) < 25:
                print("Fila incompleta, se omite:", row)
                continue
            datos = row[1:25]
            for idx in indices:
                try:
                    valor = int(datos[idx].strip())
                except Exception:
                    valor = 0
                if valor == 1:
                    suma_hc += 0.625
                elif valor == 2:
                    suma_hc += 1.25
                # Si es 0 o cualquier otro valor, se suma 0
    return suma_hc
def contar_trabajos_ar_standard(input_file):
    """
    Recorre el archivo, omite la cabecera y cuenta los "trabajos ar standard" por fila.
    Se inspeccionan las siguientes columnas de la porción row[1:25]:
      índice 1  -> ST35 W STAND AR  
      índice 5  -> ST28 W STAND AR  
      índice 9  -> 7X28 STAND AR  
      índice 13 -> 8X35 W STAND AR  
      índice 17 -> SV W STAND AR  
      índice 21 -> SV POLAR W STAND AR  
    Si el valor leído (convertido a entero) es 1 o 2 se contará como 1 trabajo.
    La función retorna la suma total de estos trabajos para todas las filas.
    """
    total_trabajos_ar_std = 0
    indices = [1, 5, 9, 13, 17, 21]
    with open(input_file, 'r', encoding='utf-8', newline='') as file:
        reader = csv.reader(file, delimiter='\t')
        next(reader, None)  # omite la cabecera
        for row in reader:
            if len(row) < 25:
                print("Fila incompleta, se omite:", row)
                continue
            datos = row[1:25]
            for idx in indices:
                try:
                    valor = int(datos[idx].strip())
                except Exception:
                    valor = 0
                if valor in (1, 2):
                    total_trabajos_ar_std += 1
    return total_trabajos_ar_std
def sumar_precio_ar_standard(input_file):
    """
    Recorre el archivo, omite la cabecera y calcula el "precio ar standard" para cada fila.
    Se toman los valores de las columnas:
      índice 1 -> ST35 W STAND AR  
      índice 5 -> ST28 W STAND AR  
      índice 9 -> 7X28 STAND AR  
      índice 13-> 8X35 W STAND AR  
      índice 17-> SV W STAND AR  
      índice 21-> SV POLAR W STAND AR  
    La regla es:
      Si el valor es 0 → aporta 0
      Si el valor es 1 → aporta 1.875
      Si el valor es 2 → aporta 3.75
    La función devuelve la suma total para todas las filas.
    """
    suma_ar_std = 0.0
    indices = [1, 5, 9, 13, 17, 21]
    with open(input_file, 'r', encoding='utf-8', newline='') as file:
        reader = csv.reader(file, delimiter='\t')
        next(reader, None)  # omite la cabecera
        for row in reader:
            if len(row) < 25:
                print("Fila incompleta, se omite:", row)
                continue
            datos = row[1:25]
            for idx in indices:
                try:
                    valor = int(datos[idx].strip())
                except Exception:
                    valor = 0
                if valor == 1:
                    suma_ar_std += 1.875
                elif valor == 2:
                    suma_ar_std += 3.75
                # Si es 0, suma 0
    return suma_ar_std
def contar_trabajos_ar_premium(input_file):
    """
    Recorre el archivo, omite la cabecera y cuenta los "trabajos ar premium" en cada fila.
    Se toman los valores de las columnas correspondientes (dentro de row[1:25]):
      índice 2  -> ST35 W PREM AR  
      índice 6  -> ST28 W PREM AR  
      índice 10 -> 7X28 W PREM AR  
      índice 14 -> 8X35 W PREM AR  
      índice 18 -> SV W PREM AR  
      índice 22 -> SV POLAR W PREM AR  
    Si el valor (convertido a entero) es 1 o 2 se cuenta como 1 registro.
    La función retorna la suma total a través de todas las filas.
    """
    total_trabajos_ar_prem = 0
    indices = [2, 6, 10, 14, 18, 22]
    with open(input_file, 'r', encoding='utf-8', newline='') as file:
        reader = csv.reader(file, delimiter='\t')
        next(reader, None)  # omite la cabecera
        for row in reader:
            if len(row) < 25:
                print("Fila incompleta, se omite:", row)
                continue
            datos = row[1:25]
            for idx in indices:
                try:
                    valor = int(datos[idx].strip())
                except Exception:
                    valor = 0
                if valor in (1, 2):
                    total_trabajos_ar_prem += 1
    return total_trabajos_ar_prem
def sumar_precio_ar_premium(input_file):
    """
    Recorre el archivo, omite la cabecera y suma el "precio ar premium" para cada fila.
    Se toman los valores de las columnas:
      índice 2  -> ST35 W PREM AR  
      índice 6  -> ST28 W PREM AR  
      índice 10 -> 7X28 W PREM AR  
      índice 14 -> 8X35 W PREM AR  
      índice 18 -> SV W PREM AR  
      índice 22 -> SV POLAR W PREM AR  
    La regla de conversión es:
      Si el valor es 0 → aporta 0  
      Si el valor es 1 → aporta 2.125  
      Si el valor es 2 → aporta 4.25  
    La función retorna la suma total a lo largo de todas las filas.
    """
    suma_ar_prem = 0.0
    indices = [2, 6, 10, 14, 18, 22]
    with open(input_file, 'r', encoding='utf-8', newline='') as file:
        reader = csv.reader(file, delimiter='\t')
        next(reader, None)  # omite la cabecera
        for row in reader:
            if len(row) < 25:
                print("Fila incompleta, se omite:", row)
                continue
            datos = row[1:25]
            for idx in indices:
                try:
                    valor = int(datos[idx].strip())
                except Exception:
                    valor = 0
                if valor == 1:
                    suma_ar_prem += 2.125
                elif valor == 2:
                    suma_ar_prem += 4.25
                # Si es 0, aporta 0 (no se suma nada)
    return suma_ar_prem
def sumar_total_precio(input_file):
    """
    Recorre el archivo, omite la cabecera y suma los valores de la última columna,
    la cual se asume que es "$$ inv". Se convierten a float para realizar la suma.
    Retorna la suma total.
    """
    suma_total = 0.0
    with open(input_file, 'r', encoding='utf-8', newline='') as file:
        reader = csv.reader(file, delimiter='\t')
        next(reader, None)  # Omitir la cabecera
        for row in reader:
            if not row:
                continue
            try:
                # Suponemos que el valor "$$ inv" es el último elemento de la fila
                valor = float(row[-1].strip())
            except Exception:
                valor = 0.0
            suma_total += valor
    return suma_total
def main_hoya():
    input_file = "I:/VISION/A_HOYTT.txt"  # Ruta interna del archivo.
    total_trabajos = contar_trabajos_tallados(input_file)
    precio_tallado = sumar_precio_tallado(input_file)
    trabajos_hc = contar_trabajos_hc(input_file)
    precio_hc = sumar_precio_hc(input_file)
    trabajos_ar_standard = contar_trabajos_ar_standard(input_file)
    precio_ar_standard = sumar_precio_ar_standard(input_file)
    trabajos_ar_premium = contar_trabajos_ar_premium(input_file)
    precio_ar_premium = sumar_precio_ar_premium(input_file)
    total_precio = sumar_total_precio(input_file)
    fecha_actual = datetime.now().strftime('%Y-%m-%d')
    # Se obtiene la semana actual; isocalendar() retorna (año, semana, día)
    semana_actual = datetime.now().isocalendar()[1]
    # Se forma el registro con todos los valores:
    # fecha, semana, trabajos_tallados, precio_tallado, trabajos_hc, precio_hc,
    # trabajos_ar_standard, precio_ar_standard, trabajos_ar_premium, precio_ar_premium, total_precio
    registro = (fecha_actual, semana_actual, total_trabajos, precio_tallado, trabajos_hc, precio_hc,
                trabajos_ar_standard, precio_ar_standard, trabajos_ar_premium, precio_ar_premium, total_precio)
    try:
        conexion = mysql.connector.connect(
            host='autorack.proxy.rlwy.net',
            port=22723,
            user='root',
            password='zsulNCCrYFSfBqIxwwIXIKqLQKFJWwbw',
            database='railway'
        )
        cursor = conexion.cursor()
        sql_insert = """
        INSERT INTO facturacion_hoyas (
            fecha, 
            semana,
            trabajos_tallados, 
            precio_tallado, 
            trabajos_hc, 
            precio_hc, 
            trabajos_ar_standard, 
            precio_ar_standard, 
            trabajos_ar_premium, 
            precio_ar_premium,
            total_precio
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(sql_insert, registro)
        conexion.commit()
        print("Registro insertado correctamente:")
        print("  trabajos_tallados    =", total_trabajos)
        print("  precio_tallado       =", precio_tallado)
        print("  trabajos_hc          =", trabajos_hc)
        print("  precio_hc            =", precio_hc)
        print("  trabajos_ar_standard =", trabajos_ar_standard)
        print("  precio_ar_standard   =", precio_ar_standard)
        print("  trabajos_ar_premium  =", trabajos_ar_premium)
        print("  precio_ar_premium    =", precio_ar_premium)
        print("  total_precio         =", total_precio)
        print("  semana               =", semana_actual)
    except mysql.connector.Error as err:
        print("Error al insertar en facturacion_hoyas:", err)
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conexion' in locals() and conexion.is_connected():
            conexion.close()
# ===================================================
# PROCESAMIENTO DEL TERCER ARCHIVO (facturacion_tercero)
# ===================================================
def process_third_file(input_file):
    """
    Procesa el archivo con los datos de facturación tercero.
    Se asume que el archivo tiene un encabezado y que los campos están separados por tabuladores.
    Debido a que la segunda columna no tiene nombre y se debe omitir, se crea un arreglo ajustado (row_adj)
    que ignora ese valor.
    Las columnas que se esperan (después de omitir la segunda) son:
      1. Patient         -> INT
      2. Lens Style      -> VARCHAR(50)
      3. Lens Material   -> VARCHAR(50)
      4. Lens Color      -> VARCHAR(50)
      5. Lens Ordered    -> INT
      6. Lens Supplied   -> INT
      7. Lens Price      -> DECIMAL(10,2)
      8. AR Coating      -> VARCHAR(50)
      9. Mirror          -> VARCHAR(50)
      10. Coatings Price -> DECIMAL(10,2)
      11. Tint           -> VARCHAR(50)
      12. Tint Ordered   -> INT
      13. Tint Price     -> DECIMAL(10,2)
      14. Job Type       -> CHAR(1)
      15. Ship Date      -> DATE (formato MM/DD/YY)
      16. TAT            -> DECIMAL 
      17. Redo           -> VARCHAR(50)
      18. Poder          -> DECIMAL(10,2)
    Se aplica la siguiente lógica:
      - Si TAT es mayor o igual a 5, se asigna 0 a LensPrice, CoatingsPrice y TintPrice.
      - Sin embargo, si el valor de Poder es mayor o igual a 12 o menor o igual a -12, se invalida la regla TAT.
    """
    registros = []
    with open(input_file, 'r', encoding='utf-8', newline='') as file:
        reader = csv.reader(file, delimiter='\t')
        encabezados = next(reader)  # omite el encabezado
        for row in reader:
            # Se espera que la fila tenga al menos 19 columnas (incluyendo la columna sin título)
            if len(row) < 19:
                print("Fila incompleta, se omite:", row)
                continue
            # Se crea un arreglo ajustado que omite la segunda columna (índice 1).
            # Es decir, row_adj contendrá: [row[0]] + row[2:]
            row_adj = [row[0]] + row[2:]
            # row_adj debe tener ahora 18 elementos.
            try:
                patient = int(row_adj[0].strip()) if row_adj[0].strip() else None
                lens_style = row_adj[1].strip() if row_adj[1].strip() else None
                lens_material = row_adj[2].strip() if row_adj[2].strip() else None
                lens_color = row_adj[3].strip() if row_adj[3].strip() else None
                try:
                    lens_ordered = int(row_adj[4].strip())
                except:
                    lens_ordered = None
                try:
                    lens_supplied = int(row_adj[5].strip())
                except:
                    lens_supplied = None
                try:
                    lens_price = float(row_adj[6].strip())
                except:
                    lens_price = None
                ar_coating = row_adj[7].strip() if row_adj[7].strip() else None
                mirror = row_adj[8].strip() if row_adj[8].strip() else None
                try:
                    coatings_price = float(row_adj[9].strip())
                except:
                    coatings_price = None
                tint = row_adj[10].strip() if row_adj[10].strip() else None
                try:
                    tint_ordered = int(row_adj[11].strip())
                except:
                    tint_ordered = None
                try:
                    tint_price = float(row_adj[12].strip())
                except:
                    tint_price = None
                job_type = row_adj[13].strip() if row_adj[13].strip() else None
                # Convertir la fecha de Ship Date, asumiendo formato MM/DD/YY
                try:
                    ship_date_obj = datetime.strptime(row_adj[14].strip(), '%m/%d/%y')
                    ship_date = ship_date_obj.strftime('%Y-%m-%d')
                except:
                    ship_date = None
                # Conversión de TAT a float (ya que en el archivo puede venir como "5.0")
                try:
                    tat = float(row_adj[15].strip())
                except Exception as e:
                    print("Error al convertir TAT:", row_adj[15], e)
                    tat = None
                redo = row_adj[16].strip() if row_adj[16].strip() else None
                try:
                    poder = float(row_adj[17].strip())
                except:
                    poder = None
                # Aplicar la condición del TAT:
                # Si TAT es mayor o igual a 5, se asigna 0 a LensPrice, CoatingsPrice y TintPrice,
                # salvo que el valor de Poder sea extremo: mayor o igual a 12 o menor o igual a -12.
                if tat is not None and tat >= 5:
                    if not (poder is not None and (poder >= 12 or poder <= -12)):
                        lens_price = 0
                        coatings_price = 0
                        tint_price = 0
                registro = (patient, lens_style, lens_material, lens_color, lens_ordered,
                            lens_supplied, lens_price, ar_coating, mirror, coatings_price,
                            tint, tint_ordered, tint_price, job_type, ship_date, tat, redo, poder)
                registros.append(registro)
            except Exception as e:
                print("Error procesando la fila:", row_adj, e)
    return registros
def main_ink():
    input_file = 'I:/VISION/A_INKREC.txt'  # Actualiza la ruta al archivo correspondiente.
    registros = process_third_file(input_file)
    if not registros:
        print("No se encontraron registros para insertar en facturacion_ink.")
        return
    try:
        conexion = mysql.connector.connect(
            host='autorack.proxy.rlwy.net',
            port=22723,
            user='root',
            password='zsulNCCrYFSfBqIxwwIXIKqLQKFJWwbw',
            database='railway'
        )
        cursor = conexion.cursor()
        # Obtenemos el número de semana a partir del día actual
        semana = datetime.now().isocalendar()[1]
        insert_query = """
        INSERT INTO facturacion_inks (
            Patient,
            LensStyle,
            LensMaterial,
            LensColor,
            LensOrdered,
            LensSupplied,
            LensPrice,
            ARCoating,
            Mirror,
            CoatingsPrice,
            Tint,
            TintOrdered,
            TintPrice,
            JobType,
            ShipDate,
            TAT,
            Redo,
            Poder,
            semana
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        for registro in registros:
            # Orden de los datos en "registro":
            # 0: Patient, 1: LensStyle, 2: LensMaterial, 3: LensColor, 4: LensOrdered,
            # 5: LensSupplied, 6: LensPrice, 7: ARCoating, 8: Mirror, 9: CoatingsPrice,
            # 10: Tint, 11: TintOrdered, 12: TintPrice, 13: JobType, 14: ShipDate,
            # 15: TAT, 16: Redo, 17: Poder.
            patient = registro[0]
            hoy = datetime.now().date()  # Se toma la fecha actual
            ship_date = hoy            # Ahora usamos la fecha de hoy en lugar de restarle un día
            tat = registro[15]
            poder = registro[17]
            aplicar_regla_tat = False
            # Buscamos duplicado: se toma en cuenta si el registro existente tiene ShipDate en un rango de 0 a 14 días respecto a hoy.
            if patient is not None and ship_date is not None:
                query = """
                SELECT id FROM facturacion_inks
                WHERE Patient = %s
                  AND DATEDIFF(%s, ShipDate) >= 0
                  AND DATEDIFF(%s, ShipDate) <= 14
                LIMIT 1
                """
                hoy_date = datetime.now().date()
                cursor.execute(query, (patient, hoy_date, hoy_date))
                duplicado = cursor.fetchone()
                if duplicado:
                    aplicar_regla_tat = True
                    update_query = """
                    UPDATE facturacion_inks
                    SET LensPrice = 0, CoatingsPrice = 0, TintPrice = 0
                    WHERE id = %s
                    """
                    cursor.execute(update_query, (duplicado[0],))
                    conexion.commit()
            registro_list = list(registro)
            if aplicar_regla_tat:
                registro_list[6] = 0   # LensPrice
                registro_list[9] = 0   # CoatingsPrice
                registro_list[12] = 0  # TintPrice
            else:
                if (tat is not None and tat >= 5) and not (poder is not None and (poder >= 12 or poder <= -12)):
                    registro_list[6] = 0
                    registro_list[9] = 0
                    registro_list[12] = 0
            registro_modificado = tuple(registro_list)
            # Se sobrescribe el valor de ShipDate (posición 14) con la fecha de hoy
            registro_final = registro_modificado[:14] + (str(hoy),) + registro_modificado[15:] + (semana,)
            cursor.execute(insert_query, registro_final)
            conexion.commit()
        print("Se han procesado e insertado los registros en facturacion_ink.")
    except mysql.connector.Error as err:
        print("Error en MySQL (facturacion_inks):", err)
    except Exception as ex:
        print("Error:", ex)
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conexion' in locals() and conexion.is_connected():
            conexion.close()
# ===============================================
# BLOQUE PRINCIPAL: LLAMADA A LOS 3 PROCESOS
# ===============================================
if __name__ == '__main__':
    from datetime import datetime
    # Si se usa isoweekday(), el domingo es 7
    if datetime.now().isoweekday() == 7:
        print("Hoy es domingo. No se insertarán datos.")
    else:
        print("Procesando archivo de facturación NVI...")
        main_nvi()
        print("Procesando archivo para acumulado en facturacion_hoyas...")
        main_hoya()
        print("Procesando archivo de facturación tercero...")
        main_ink()