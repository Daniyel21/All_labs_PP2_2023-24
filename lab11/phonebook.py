import psycopg2
import csv
from tabulate import tabulate 

conn = psycopg2.connect(host="localhost", dbname="lab10", user="postgres",
                        password="D@niko123", port=5432)

cur = conn.cursor()

# Create table and procedures
cur.execute("""CREATE TABLE IF NOT EXISTS phonebook (
      user_id SERIAL PRIMARY KEY,
      name VARCHAR(255) NOT NULL,
      surname VARCHAR(255) NOT NULL, 
      phone VARCHAR(255) NOT NULL
)
""")

# Create procedures
cur.execute("""
CREATE OR REPLACE PROCEDURE insert_or_update_contact(
    p_name VARCHAR, 
    p_surname VARCHAR, 
    p_phone VARCHAR
)
AS $$
BEGIN
    IF (p_phone LIKE '+%' OR p_phone LIKE '8%') AND LENGTH(p_phone) = 11 THEN
        IF EXISTS (SELECT 1 FROM phonebook WHERE name = p_name AND surname = p_surname) THEN
            UPDATE phonebook SET phone = p_phone WHERE name = p_name AND surname = p_surname;
        ELSE
            INSERT INTO phonebook (name, surname, phone) VALUES (p_name, p_surname, p_phone);
        END IF;
    ELSE
        RAISE EXCEPTION 'Invalid phone number format. Should start with + or 8 and be 11 digits.';
    END IF;
END;
$$ LANGUAGE plpgsql;
""")

cur.execute("""
CREATE OR REPLACE PROCEDURE update_contact(
    p_column VARCHAR,
    p_old_value VARCHAR,
    p_new_value VARCHAR
)
AS $$
BEGIN
    EXECUTE format('UPDATE phonebook SET %I = $1 WHERE %I = $2', p_column, p_column)
    USING p_new_value, p_old_value;
    
    -- Reset sequence and renumber IDs
    PERFORM setval(pg_get_serial_sequence('phonebook', 'user_id'), 1, false);
    
    UPDATE phonebook
    SET user_id = new_id
    FROM (
        SELECT user_id, ROW_NUMBER() OVER (ORDER BY user_id) as new_id 
        FROM phonebook
    ) AS renumbered
    WHERE phonebook.user_id = renumbered.user_id;
    
    PERFORM setval(
        pg_get_serial_sequence('phonebook', 'user_id'), 
        COALESCE((SELECT MAX(user_id) FROM phonebook), 0) + 1
    );
END;
$$ LANGUAGE plpgsql;
""")

cur.execute("""
CREATE OR REPLACE PROCEDURE delete_contact(
    p_search_term VARCHAR
)
AS $$
DECLARE
    v_deleted_count INTEGER;
BEGIN
    DELETE FROM phonebook 
    WHERE phone = p_search_term OR name = p_search_term
    RETURNING user_id INTO v_deleted_count;
    
    IF v_deleted_count > 0 THEN
        -- Reset sequence and renumber IDs
        PERFORM setval(pg_get_serial_sequence('phonebook', 'user_id'), 1, false);
        
        UPDATE phonebook
        SET user_id = new_id
        FROM (
            SELECT user_id, ROW_NUMBER() OVER (ORDER BY user_id) as new_id 
            FROM phonebook
        ) AS renumbered
        WHERE phonebook.user_id = renumbered.user_id;
        
        PERFORM setval(
            pg_get_serial_sequence('phonebook', 'user_id'), 
            COALESCE((SELECT MAX(user_id) FROM phonebook), 0) + 1
        );
    END IF;
END;
$$ LANGUAGE plpgsql;
""")

cur.execute("""
CREATE OR REPLACE PROCEDURE renumber_ids()
AS $$
BEGIN
    -- Reset sequence and renumber IDs
    PERFORM setval(pg_get_serial_sequence('phonebook', 'user_id'), 1, false);
    
    UPDATE phonebook
    SET user_id = new_id
    FROM (
        SELECT user_id, ROW_NUMBER() OVER (ORDER BY user_id) as new_id 
        FROM phonebook
    ) AS renumbered
    WHERE phonebook.user_id = renumbered.user_id;
    
    PERFORM setval(
        pg_get_serial_sequence('phonebook', 'user_id'), 
        COALESCE((SELECT MAX(user_id) FROM phonebook), 0) + 1
    );
END;
$$ LANGUAGE plpgsql;
""")

conn.commit()

def insert_data():
    print('Type "csv" or "con" to choose option between uploading csv file or typing from console: ')
    method = input().lower()
    
    if method == "con":
        name = input("Name: ")
        surname = input("Surname: ")
        phone = input("Phone: ")
        try:
            cur.callproc("insert_or_update_contact", (name, surname, phone))
            conn.commit()
            print(f"Successfully processed {name} {surname}")
        except Exception as e:
            print(f"Error: {e}")
            conn.rollback()

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
                        try:
                            cur.callproc("insert_or_update_contact", (name, surname, phone))
                            processed_rows += 1
                        except:
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
    try:
        cur.callproc("update_contact", (column, value, new_value))
        conn.commit()
        print("Update successful")
    except Exception as e:
        print(f"Error updating data: {e}")
        conn.rollback()

def delete_data():
    search_term = input('Write phone number or name to delete: ').strip()
    
    try:
        cur.execute("""
            SELECT * FROM phonebook 
            WHERE phone = %s OR name = %s
        """, (search_term, search_term))
        records = cur.fetchall()
        
        if not records:
            print("No matches found")
            return
            
        print("Records found to be deleted:")
        print(tabulate(records, headers=["ID", "Name", "Surname", "Phone"], tablefmt="fancy_grid"))
        
        confirm = input("Are you sure? (y/n): ").lower()
        if confirm != 'y':
            print("Deletion cancelled")
            return

        cur.callproc("delete_contact", (search_term,))
        conn.commit()
        print(f"Deleted {len(records)} records")
        
    except Exception as e:
        print(f"Error deleting data: {e}")
        conn.rollback()

def query_by_pattern():
    pattern = input("Write part of name, surname or phone: ")
    cur.execute("""
        SELECT * FROM phonebook 
        WHERE name ILIKE %s OR surname ILIKE %s OR phone LIKE %s
    """, (f'%{pattern}%', f'%{pattern}%', f'%{pattern}%'))
    rows = cur.fetchall()
    print(tabulate(rows, headers=["ID", "Name", "Surname", "Phone"]))

def display_data():
    try:
        cur.callproc("renumber_ids")
        conn.commit()
        cur.execute("SELECT * from phonebook;")
        rows = cur.fetchall()
        print(tabulate(rows, headers=["ID", "Name", "Surname", "Phone"], tablefmt='fancy_grid'))
    except Exception as e:
        print(f"Error displaying data: {e}")
        conn.rollback()

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