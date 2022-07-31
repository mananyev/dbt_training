{% set database=target.dbname %}

{% snapshot orders_snapshot %}

{{
    config(
      target_database=database,
      target_schema='snapshots',
      unique_key='id',

      strategy='timestamp',
      updated_at='order_date',
    )
}}

select * from {{ source('jaffle_shop', 'orders') }}

{% endsnapshot %}