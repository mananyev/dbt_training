{% macro create_tables_amazon_shopify() %}

    {% set shops = ['amazon', 'shopify', ]%}

    {% set queries %}

        
    {% for shop in shops %}

    DROP TABLE IF EXISTS "stripe"."orders__{{ shop }}";

    CREATE TABLE IF NOT EXISTS "stripe"."orders__{{ shop }}"(
        "order_id" INTEGER NOT NULL,
        "order_amount" INTEGER NOT NULL,
        "order_date" TIMESTAMP NOT NULL
    ) ENCODE AUTO;

    {% endfor %}

    INSERT INTO "stripe"."orders__shopify" VALUES
        (1, 45, '2021-03-24T19:29:23'),
        (2, 35, '2021-03-25T09:31:38');

    INSERT INTO "stripe"."orders__amazon" VALUES
        (3, 55, '2021-03-26T01:26:23'),
        (4, 150, '2021-04-24T19:12:52');

    {% endset %}

    {% do run_query(queries) %}

{% endmacro %};