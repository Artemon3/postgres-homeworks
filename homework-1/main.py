"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import csv

conn = psycopg2.connect(host='localhost', database='north', user='postgres', password='')

with conn.cursor() as cur:
    with open('north_data/employees_data.csv', "r", encoding='utf-8') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            employee_id = row['employee_id']
            first_name = row['first_name']
            last_name = row['last_name']
            title = row['title']
            birth_date = row['birth_date']
            notes = row['notes']
            cur.execute('INSERT INTO employees VALUES(%s, %s, %s, %s, %s, %s)', (
                employee_id,
                first_name,
                last_name,
                title,
                birth_date,
                notes)
            )
            cur.execute('SELECT * FROM employees')
            conn.commit()
            rows = cur.fetchall()

            for one in rows:
                print(one)



with conn.cursor() as cur:
    with open('north_data/customers_data.csv', "r", encoding='utf-8') as csv_file:
        reader = csv.reader(csv_file)
        next(reader)
        for row in reader:
            cur.execute('INSERT INTO customers (customer_id, company_name, contact_name) VALUES(%s, %s, %s)', row)

with conn.cursor() as cur:
    with open('north_data/orders_data.csv', "r", encoding='utf-8') as csv_file:
        reader = csv.reader(csv_file)
        next(reader)
        for row in reader:
            cur.execute('INSERT INTO orders (order_id, company_id, employee_id, order_date, ship_city) '
                        'VALUES(%s, %s, %s, %s, %s)', row)


conn.commit()
cur.close()
conn.close()