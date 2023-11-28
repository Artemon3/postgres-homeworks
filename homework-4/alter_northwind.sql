-- Подключиться к БД Northwind и сделать следующие изменения:
-- 1. Добавить ограничение на поле unit_price таблицы products (цена должна быть больше 0)
ALTER TABLE products ADD CHECK (unit_price > 0)


-- 2. Добавить ограничение, что поле discontinued таблицы products может содержать только значения 0 или 1
ALTER TABLE products ADD CHECK (discontinued = 0 OR discontinued = 1)


-- 3. Создать новую таблицу, содержащую все продукты, снятые с продажи (discontinued = 1)
SELECT INTO new_products FROM products WHERE discontinued = 1 -- так не работает у меня, выдает пустую таблицу из 10 строк

CREATE TABLE n_products AS SELECT * FROM products WHERE discontinued = 1 -- так все хорошо

-- 4. Удалить из products товары, снятые с продажи (discontinued = 1)
-- Для 4-го пункта может потребоваться удаление ограничения, связанного с foreign_key. Подумайте, как это можно решить, чтобы связь с таблицей order_details все же осталась.
ALTER TABLE order_details DROP CONSTRAINT fk_order_details_products 
DELETE FROM products WHERE discontinued = 1
DELETE FROM order_details WHERE product_id NOT IN (SELECT product_id FROM products)
DELETE FROM orders WHERE order_id NOT IN (SELECT order_id FROM order_details)
ALTER TABLE order_details ADD CONSTRAINT fk_order_details FOREIGN KEY(product_id) REFERENCES products(product_id)
