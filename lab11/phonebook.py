import psycopg2
import csv
from tabulate import tabulate 

conn = psycopg2.connect(host="localhost", dbname="lab10", user="postgres",
                        password="D@niko123", port=5432)

cur = conn.cursor()

cur.execute("""CREATE TABLE IF NOT EXISTS phonebook (
      user_id SERIAL PRIMARY KEY,
      name VARCHAR(255) NOT NULL,
      surname VARCHAR(255) NOT NULL, 
      phone VARCHAR(255) NOT NULL
)
""")

def insert_data():
    print('Type "csv" or "con" to choose option between uploading csv file or typing from console: ')
    method = input().lower()
    
    if method == "con":
        name = input("Name: ")
        surname = input("Surname: ")
        phone = input("Phone: ")
        if (phone.startswith('+') or phone.startswith('8')) and len(phone) == 11:
            cur.execute("SELECT * FROM phonebook WHERE name = %s AND surname = %s", (name, surname))
            existing = cur.fetchone()
            if existing:
                cur.execute("UPDATE phonebook SET phone = %s WHERE name = %s AND surname = %s", 
                           (phone, name, surname))
                print(f"Updated phone for {name} {surname}")
            else:
                cur.execute("INSERT INTO phonebook (name, surname, phone) VALUES (%s, %s, %s)", 
                           (name, surname, phone))
                print(f"Inserted {name} {surname}")
            conn.commit()
        else:
            print("Invalid phone number format. Should start with + or 8 and be 11 digits.")

    elif method == "csv":
        filepath = input("Enter a file path with proper extension: ")
        invalid_rows = []
        processed_rows = 0

        try:
            with open(filepath, 'r', newline='') as f:
                reader = csv.reader(f)
                for row in reader:
                    if len(row) >= 3:
                        name, surname, phone = row[0].strip(), row[1].strip(), row[2].strip()
                        if (phone.startswith('+') or phone.startswith('8')) and len(phone) == 11:
                            cur.execute("SELECT * FROM phonebook WHERE name = %s AND surname = %s", 
                                       (name, surname))
                            existing = cur.fetchone()
                            if existing:
                                cur.execute("UPDATE phonebook SET phone = %s WHERE name = %s AND surname = %s", 
                                           (phone, name, surname))
                            else:
                                cur.execute("INSERT INTO phonebook (name, surname, phone) VALUES (%s, %s, %s)", 
                                           (name, surname, phone))
                            processed_rows += 1
                        else:
                            invalid_rows.append(f"Invalid phone format in row: {row}")
                    else:
                        invalid_rows.append(f"Incomplete data in row: {row}")
            
            conn.commit()
            
            if invalid_rows:
                print("\nThe following rows had errors:")
                for error in invalid_rows:
                    print(f" - {error}")
            
            print(f"\nSuccessfully processed {processed_rows} records")
            
        except FileNotFoundError:
            print("File not found. Please check the file path and try again.")
        except Exception as e:
            print(f"Error reading file: {e}")
            conn.rollback()
def update_data():
    column = input('Type the name of the column that you want to change: ')
    value = input(f"Enter {column} that you want to change: ")
    new_value = input(f"Enter the new {column}: ")
    cur.execute(f"UPDATE phonebook SET {column} = %s WHERE {column} = %s", (new_value, value))
    # Сброс автоинкремента user_id
    cur.execute("SELECT pg_get_serial_sequence('phonebook', 'user_id')")
    seq_name = cur.fetchone()[0]
    cur.execute(f"ALTER SEQUENCE {seq_name} RESTART WITH 1")
    
    # Обновление всех оставшихся user_id
    cur.execute("""
        WITH cte AS (SELECT user_id, ROW_NUMBER() OVER (ORDER BY user_id) as new_id FROM phonebook)
        UPDATE phonebook
        SET user_id = cte.new_id
        FROM cte
        WHERE phonebook.user_id = cte.user_id
    """)
    # Получаем максимальное значение user_id
    cur.execute("SELECT MAX(user_id) FROM phonebook")
    max_id = cur.fetchone()[0] or 0
    
    # Устанавливаем следующий user_id в последовательности
    cur.execute("SELECT pg_get_serial_sequence('phonebook', 'user_id')")
    seq_name = cur.fetchone()[0]
    cur.execute(f"ALTER SEQUENCE {seq_name} RESTART WITH {max_id + 1}")
    conn.commit()

def delete_data():
    search_term = input('Write phone number or name to delete: ').strip()
    
    try:
        cur.execute("""
            SELECT * FROM phonebook 
            WHERE phone = %s OR name = %s
        """, (search_term, search_term))
        records = cur.fetchall()
        
        if not records:
            print("No matches found ")
            return
            
        print("Records found to be deleted:")
        print(tabulate(records, headers=["ID", "Name", "Surname", "Phone"],tablefmt="fancy_grid"))
        
        confirm = input("Are you sure? (y/n): ").lower()
        if confirm != 'y':
            print("Undo deletion")
            return

        cur.execute("""
            DELETE FROM phonebook 
            WHERE phone = %s OR name = %s
            RETURNING user_id
        """, (search_term, search_term))
        deleted_ids = [row[0] for row in cur.fetchall()]
        
        if not deleted_ids:
            print("Nothing has been removed")
            conn.rollback()
            return
            
        print(f"Records removed: {len(deleted_ids)}")
        conn.commit()

        if deleted_ids:
            cur.execute("SELECT COALESCE(MIN(user_id), 0) FROM phonebook")
            min_id = cur.fetchone()[0]
            
            if min_id > 1:
                cur.execute("""
                    UPDATE phonebook SET user_id = new_id
                    FROM (
                        SELECT user_id, ROW_NUMBER() OVER (ORDER BY user_id) as new_id 
                        FROM phonebook
                    ) as renumbered
                    WHERE phonebook.user_id = renumbered.user_id
                    AND phonebook.user_id != renumbered.new_id
                """)
                conn.commit()

                cur.execute("SELECT MAX(user_id) FROM phonebook")
                max_id = cur.fetchone()[0] or 0
                cur.execute(f"ALTER SEQUENCE phonebook_user_id_seq RESTART WITH {max_id + 1}")
                conn.commit()
                
    except Exception as e:
        print(f"Ошибка при удалении: {e}")
        conn.rollback()

def query_by_pattern():
    pattern = input("Write part of name, surname or phone: ")
    cur.execute("""
        SELECT * FROM phonebook 
        WHERE name ILIKE %s OR surname ILIKE %s OR phone LIKE %s
    """, (f'%{pattern}%', f'%{pattern}%', f'%{pattern}%'))
    rows = cur.fetchall()
    print(tabulate(rows, headers=["ID", "Имя", "Фамилия", "Телефон"]))

def display_data():
    # Сброс автоинкремента user_id
    cur.execute("SELECT pg_get_serial_sequence('phonebook', 'user_id')")
    seq_name = cur.fetchone()[0]
    cur.execute(f"ALTER SEQUENCE {seq_name} RESTART WITH 1")
    
    # Обновление всех оставшихся user_id
    cur.execute("""
        WITH cte AS (
            SELECT user_id, ROW_NUMBER() OVER (ORDER BY user_id) as new_id
            FROM phonebook
        )
        UPDATE phonebook
        SET user_id = cte.new_id
        FROM cte
        WHERE phonebook.user_id = cte.user_id
    """)
    # Получаем максимальное значение user_id
    cur.execute("SELECT MAX(user_id) FROM phonebook")
    max_id = cur.fetchone()[0] or 0
    
    # Устанавливаем следующий user_id в последовательности
    cur.execute("SELECT pg_get_serial_sequence('phonebook', 'user_id')")
    seq_name = cur.fetchone()[0]
    cur.execute(f"ALTER SEQUENCE {seq_name} RESTART WITH {max_id + 1}")
    conn.commit()
    cur.execute("SELECT * from phonebook;")
    rows = cur.fetchall()
    print(tabulate(rows, headers=["ID", "Name", "Surname", "Phone"], tablefmt='fancy_grid'))

while True:
    print("""
    List of the commands:
    1. Type "i" or "I" in order to INSERT data to the table.
    2. Type "u" or "U" in order to UPDATE data in the table.
    3. Type "q" or "Q" in order to make specific QUERY in the table.
    4. Type "d" or "D" in order to DELETE data from the table.
    5. Type "s" or "S" in order to see the values in the table.
    6. Type "f" or "F" in order to close the program.
    """)

    command = input().lower()

    if command == "i":
        insert_data()
    elif command == "u":
        update_data()
    elif command == "d":
        delete_data()
    elif command == "q":
        query_by_pattern()
    elif command == "s":
        display_data()
    elif command == "f":
        break

conn.commit()
cur.close()
conn.close()