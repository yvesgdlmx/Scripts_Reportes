import csv
import mysql.connector
from datetime import datetime, timedelta

def extract_date_from_string(date_str):
    try:
        date_str = date_str.strip()
        # Intentar primero con el formato MM/DD/YY y MM/DD/YYYY
        try:
            date_obj = datetime.strptime(date_str, '%m/%d/%y')
            if date_obj.year < 100:
                date_obj = date_obj.replace(year=date_obj.year + 2000)
            return date_obj.strftime('%Y-%m-%d')
        except ValueError:
            pass

        try:
            date_obj = datetime.strptime(date_str, '%m/%d/%Y')
            return date_obj.strftime('%Y-%m-%d')
        except ValueError:
            pass

        # Intentar con el formato DD/MM/YY y DD/MM/YYYY
        try:
            date_obj = datetime.strptime(date_str, '%d/%m/%y')
            if date_obj.year < 100:
                date_obj = date_obj.replace(year=date_obj.year + 2000)
            return date_obj.strftime('%Y-%m-%d')
        except ValueError:
            pass

        try:
            date_obj = datetime.strptime(date_str, '%d/%m/%Y')
            return date_obj.strftime('%Y-%m-%d')
        except ValueError:
            pass

        raise ValueError(f"No se pudo convertir la fecha: {date_str}")
    except Exception as e:
        print(f"Error al procesar la fecha {date_str}: {e}")
        return None

def clean_value(value):
    if value.strip() == '':
        return 0
    try:
        return float(value)
    except ValueError:
        return 0

def get_existing_record(cursor, enter_date, today):
    query = "SELECT enter_date FROM antiguedades WHERE enter_date = %s AND today = %s"
    cursor.execute(query, (enter_date, today))
    return cursor.fetchone()

def delete_existing_record(cursor, enter_date, today):
    query = "DELETE FROM antiguedades WHERE enter_date = %s AND today = %s"
    cursor.execute(query, (enter_date, today))

def main():
    input_file = 'I:/VISION/A_QALL.txt'
    data = []
    today = datetime.now().strftime('%Y-%m-%d')
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
        with open(input_file, 'r') as file:
            reader = csv.reader(file, delimiter='\t')
            next(reader)  # Saltar la cabecera
            for row in reader:
                if len(row) < 10:  # Asegurarse de que haya suficientes columnas
                    print(f"Fila ignorada por falta de campos: {row}")
                    continue
                enter_date = extract_date_from_string(row[0])
                if not enter_date:
                    print(f"Fila ignorada por fecha inválida: {row}")
                    continue
                try:
                    processed_row = (
                        enter_date,
                        today,
                        clean_value(row[1]),  # INK QUEUE
                        int(clean_value(row[2])),  # INK IP
                        clean_value(row[3]),  # HOYA QUEUE
                        int(clean_value(row[4])),  # HOYA IP
                        clean_value(row[5]),  # NVI QUEUE
                        int(clean_value(row[6])),  # NVI IP
                        clean_value(row[9])  # DIGITAL CALCULATOR
                    )
                    existing_record = get_existing_record(cursor, enter_date, today)
                    if existing_record:
                        delete_existing_record(cursor, enter_date, today)
                    data.append(processed_row)
                    print(f"Datos procesados para la fecha {enter_date}: {processed_row}")
                except Exception as e:
                    print(f"Error al procesar la fila {row}: {e}")
                    continue
        insert_query = """
        INSERT INTO antiguedades (enter_date, today, ink_queue, ink_ip, hoya_queue, hoya_ip,
                             nvi_queue, nvi_ip, digital_calculator)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_query, data)
        connection.commit()
        print(f"Se insertaron {len(data)} registros exitosamente.")
    except mysql.connector.Error as err:
        print(f"Error de MySQL: {err}")
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()
            print("Conexión cerrada.")

if __name__ == "__main__":
    main()