-- Создание слоя raw
CREATE DATABASE raw;

-- Создание таблицы для данных из Объектоного хранилища
CREATE TABLE raw.site_visits (
date              DateTime,
timestamp         DateTime,
user_client_id    Int64,
action_type       String,
placement_type    String,
placement_id      Int64,
user_visit_url    String,
insert_time DateTime,
hash String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY date

-- Создание таблицы для хранения транзакций из MySQL
CREATE TABLE raw.user_payments(
date              DateTime,
timestamp         DateTime,
user_client_id    Int64,
item              String,
price             Int64,
quantity          Int64,
amount            Float64,
discount          Float64,
order_id          Int64,
status            String,
insert_time DateTime,
hash String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY date