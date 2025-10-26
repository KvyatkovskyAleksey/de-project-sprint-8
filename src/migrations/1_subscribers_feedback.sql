-- Выходная таблица
-- DROP TABLE public.subscribers_feedback;

CREATE TABLE public.subscribers_feedback (
  id serial4 NOT NULL,
    restaurant_id text NOT NULL,
    adv_campaign_id text NOT NULL,
    adv_campaign_content text NOT NULL,
    adv_campaign_owner text NOT NULL,
    adv_campaign_owner_contact text NOT NULL,
    adv_campaign_datetime_start int8 NOT NULL,
    adv_campaign_datetime_end int8 NOT NULL,
    datetime_created int8 NOT NULL,
    client_id text NOT NULL,
    trigger_datetime_created int4 NOT NULL,
    feedback varchar NULL,
    CONSTRAINT id_pk PRIMARY KEY (id)
);

-- Пример заполненных данных
-- id|restaurant_id                       |adv_campaign_id                     |adv_campaign_content|adv_campaign_owner   |adv_campaign_owner_contact|adv_campaign_datetime_start|adv_campaign_datetime_end|datetime_created|client_id                           |trigger_datetime_created|feedback|
-- --+------------------------------------+------------------------------------+--------------------+---------------------+--------------------------+---------------------------+-------------------------+----------------+------------------------------------+------------------------+--------+
--  1|123e4567-e89b-12d3-a456-426614174000|123e4567-e89b-12d3-a456-426614174003|first campaign      |Ivanov Ivan Ivanovich|iiivanov@restaurant.ru    |                 1659203516|               2659207116|      1659131516|223e4567-e89b-12d3-a456-426614174000|              1659304828|        |
--  2|123e4567-e89b-12d3-a456-426614174000|123e4567-e89b-12d3-a456-426614174003|first campaign      |Ivanov Ivan Ivanovich|iiivanov@restaurant.ru    |                 1659203516|               2659207116|      1659131516|323e4567-e89b-12d3-a456-426614174000|              1659304828|        |
--  3|123e4567-e89b-12d3-a456-426614174000|123e4567-e89b-12d3-a456-426614174003|first campaign      |Ivanov Ivan Ivanovich|iiivanov@restaurant.ru    |                 1659203516|               2659207116|      1659131516|423e4567-e89b-12d3-a456-426614174000|              1659304828|        |
--  4|123e4567-e89b-12d3-a456-426614174000|123e4567-e89b-12d3-a456-426614174003|first campaign      |Ivanov Ivan Ivanovich|iiivanov@restaurant.ru    |                 1659203516|               2659207116|      1659131516|523e4567-e89b-12d3-a456-426614174000|              1659304828|        |
--  5|123e4567-e89b-12d3-a456-426614174000|123e4567-e89b-12d3-a456-426614174003|first campaign      |Ivanov Ivan Ivanovich|iiivanov@restaurant.ru    |                 1659203516|               2659207116|      1659131516|623e4567-e89b-12d3-a456-426614174000|              1659304828|        |
--  6|123e4567-e89b-12d3-a456-426614174000|123e4567-e89b-12d3-a456-426614174003|first campaign      |Ivanov Ivan Ivanovich|iiivanov@restaurant.ru    |                 1659203516|               2659207116|      1659131516|723e4567-e89b-12d3-a456-426614174000|              1659304828|        |
--  7|123e4567-e89b-12d3-a456-426614174000|123e4567-e89b-12d3-a456-426614174003|first campaign      |Ivanov Ivan Ivanovich|iiivanov@restaurant.ru    |                 1659203516|               2659207116|      1659131516|823e4567-e89b-12d3-a456-426614174000|              1659304828|        |
--  8|123e4567-e89b-12d3-a456-426614174000|123e4567-e89b-12d3-a456-426614174003|first campaign      |Ivanov Ivan Ivanovich|iiivanov@restaurant.ru    |                 1659203516|               2659207116|      1659131516|023e4567-e89b-12d3-a456-426614174000|              1659304828|        |