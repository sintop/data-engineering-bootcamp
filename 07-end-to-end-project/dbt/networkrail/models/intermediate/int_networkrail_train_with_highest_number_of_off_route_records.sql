with

fct_movements as (

    select * from {{ ref("fct_movements") }}

)

, highest_off_route as (

    select
       train_id
       ,company_name
       ,count(variation_status) as cnt
    from fct_movements where variation_status = 'OFF ROUTE'
    group by 1,2 
    order by 3 desc 
    limit 1
  

)

select * from highest_off_route