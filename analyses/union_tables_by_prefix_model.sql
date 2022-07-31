{{ create_tables_amazon_shopify() }}

{{
    union_tables_by_prefix(
        database=target.dbname,
        schema='stripe',
        prefix='orders__'
    )
}}