-- Создание слоя tmp
CREATE DATABASE tmp;

-- Создание таблицы для данных из Объектоного хранилища
CREATE TABLE tmp.site_visits  (
date              String,
timestamp         String,
user_client_id    Int64,
action_type       String,
placement_type    String,
placement_id      Int64,
user_visit_url    String
) ENGINE = Log

-- Создание таблицы для хранения транзакций из MySQL
CREATE TABLE tmp.user_payments (
date              String,
timestamp         String,
user_client_id    Int64,
item              String,
price             Int64,
quantity          Int64,
amount            Float64,
discount          Float64,
order_id          Int64,
status            String
) ENGINE = Log