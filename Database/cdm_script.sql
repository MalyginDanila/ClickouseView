--Создание слоя витрин
CREATE DATABASE сdm;

--Создание витрины user_visits
create VIEW if not EXISTS cdm.user_visits AS
Select
    toDate(date) as date,
    action_type,
    placement_type,
    user_visit_url,
    count(distinct user_client_id) as cnt_uniq_users,
    count(user_client_id) as cnt_actions
FROM
    raw.site_visits
GROUP BY 
    date,
    action_type,
    placement_type,
    user_visit_url

--создаём таблицу записи данных
CREATE TABLE IF NOT EXISTS cdm.item_payments
        (`date` Date,
        `item` LowCardinality(String),
        `status` LowCardinality(String),
        `total_amount` Float32,
        `total_quantity` UInt32,
        `total_discount` Float32,
        `cnt_uniq_users` UInt16,
        `cnt_statuses`   UInt16
       )
ENGINE = SummingMergeTree
ORDER BY (date, item,status);

--создаём материализованное представление записи данных при поступлении из источников в витрину
CREATE MATERIALIZED VIEW if not exists cdm.item_payments_mv TO cdm.item_payments AS
SELECT
    toDate(date) AS date,
    item,
    status,
    sum(amount) AS total_amount,
    sum(quantity) AS total_quantity,
    sum(price*quantity) - sum(amount) AS total_discount,
    count(distinct user_client_id) AS cnt_uniq_users,
    count(user_client_id) AS cnt_statuses
FROM raw.user_payments
GROUP BY date, item, status;

--инициируем первую вставку данных
INSERT INTO cdm.item_payments SELECT
    toDate(date) AS date,
    item,
    status,
    sum(amount) AS total_amount,
    sum(quantity) AS total_quantity,
    sum(price*quantity) - sum(amount) AS total_discount,
    count(DISTINCT user_client_id) AS cnt_uniq_users,
    count(user_client_id) AS cnt_statuses
FROM raw.user_payments
GROUP BY date, item, status;

