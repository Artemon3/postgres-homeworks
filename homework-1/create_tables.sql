-- SQL-команды для создания таблиц
CREATE TABLE customers
(
customer_id varchar(100) PRIMARY KEY,
company_name varchar(100) NOT NULL,
contact_name varchar(100) NOT NULL
)
SELECT * FROM customers

CREATE TABLE orders
(
order_id int PRIMARY KEY,
company_id varchar REFERENCES customers(customer_id),
employee_id int REFERENCES employees(employee_id),
order_date date,
ship_city varchar
)

SELECT * FROM customers

CREATE TABLE employees
(
employee_id int PRIMARY KEY,
first_name varchar(100) NOT NULL,
last_name varchar(100) NOT NULL,
title varchar(100) NOT NULL,
birth_date date,
notes text
)

SELECT * FROM employees
