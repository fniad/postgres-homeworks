"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import csv
from dateutil import parser


def connect_to_database():
    """ Соединение с базой данных """
    try:
        conn = psycopg2.connect(host='localhost', database='north', user='postgres', password='04091997')
        return conn, conn.cursor()
    except psycopg2.Error as e:
        print(f'Ошибка при подключении к базе данных: {e}')
        return None, None


def load_data_to_table_employees(conn, cur):
    """ Загрузка данных из таблицы employees """
    try:
        with open('north_data/employees_data.csv') as file:
            csv_data = csv.reader(file)
            next(csv_data)

            for row in csv_data:
                employee_id = int(row[0])
                first_name = row[1]
                last_name = row[2]
                title = row[3]
                birth_date = parser.parse(row[4]).date()
                notes = row[5]

                cur.execute('''INSERT INTO employees 
                               ("employee_id","first_name","last_name","title","birth_date","notes") 
                               VALUES (%s, %s, %s, %s, %s, %s)''',
                            (employee_id, first_name, last_name, title, birth_date, notes))

            conn.commit()
            print('Данные успешно загружены в таблицу.')
    except FileNotFoundError:
        print('Файл .csv не найден.')
    except psycopg2.Error as e:
        print(f'Ошибка при выполнении запроса: {e}')


def load_data_to_table_customers(conn, cur):
    """ Загрузка данных из таблицы customers """
    try:
        with open('north_data/customers_data.csv') as file:
            csv_data = csv.reader(file)
            next(csv_data)

            for row in csv_data:
                cur.execute('''INSERT INTO customers
                               ("customer_id","company_name","contact_name") 
                               VALUES (%s, %s, %s)''', row)

            # Зафиксировать изменения в базе данных
            conn.commit()
            print('Данные успешно загружены в таблицу.')
    except FileNotFoundError:
        print('Файл .csv не найден.')
    except psycopg2.Error as e:
        print(f'Ошибка при выполнении запроса: {e}')


def load_data_to_table_orders(conn, cur):
    """ Загрузка данных из таблицы order """
    try:
        with open('north_data/orders_data.csv') as file:
            csv_data = csv.reader(file)
            next(csv_data)

            for row in csv_data:
                order_id = int(row[0])
                customer_id = row[1]
                employee_id = row[2]
                order_date = parser.parse(row[3]).date()
                ship_city = row[4]

                cur.execute('''INSERT INTO orders
                               ("order_id","customer_id","employee_id","order_date","ship_city") 
                               VALUES (%s, %s, %s, %s, %s)''',
                            (order_id, customer_id, employee_id, order_date, ship_city))

            conn.commit()
            print('Данные успешно загружены в таблицу.')
    except FileNotFoundError:
        print('Файл .csv не найден.')
    except psycopg2.Error as e:
        print(f'Ошибка при выполнении запроса: {e}')


def main():
    conn, cur = connect_to_database()
    if conn is not None and cur is not None:
        load_data_to_table_employees(conn, cur)
        load_data_to_table_customers(conn, cur)
        load_data_to_table_orders(conn, cur)
        cur.close()
        conn.close()


if __name__ == '__main__':
    main()
