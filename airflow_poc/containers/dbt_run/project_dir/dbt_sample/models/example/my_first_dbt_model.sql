
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/


{{ config(materialized='table') }}

with source_data as (

    select "0" as id
    union all
    select "2" as id
    union all
    select '{{ var("elt_ts") }}' as id
--    select "dofd" as id
)

{{ log("Running some_macro: " ~ arg1 ~ ", " ~ arg2) }}

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
