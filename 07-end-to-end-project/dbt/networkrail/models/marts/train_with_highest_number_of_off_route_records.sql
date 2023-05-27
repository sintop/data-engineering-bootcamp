with

int_networkrail_train_with_highest_number_of_off_route_records as (

    select * from {{ ref('int_networkrail_train_with_highest_number_of_off_route_records') }}

)

, final as (

    select
        train_id
        , company_name
        , cnt

    from int_networkrail_train_with_highest_number_of_off_route_records

)

select * from final