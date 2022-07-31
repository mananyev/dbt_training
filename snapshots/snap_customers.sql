{% snapshot customers_snapshot %}

{{
    config(
      target_database='dbtworkshop',
      target_schema='snapshots',
      unique_key='id',

      strategy='check',
      check_cols=['first_name', 'last_name', ],
    )
}}

select * from {{ source('jaffle_shop', 'customers') }}

{% endsnapshot %}