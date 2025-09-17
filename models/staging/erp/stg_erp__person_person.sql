with
    source_person as (
        select *
        from {{ source('erp', 'person_person') }}
    )

    , renamed as (
        select
            cast(businessentityid as int) as person_pk
            , cast(persontype as string) as person_type
            , cast(namestyle as boolean) as person_name_style
            , cast(title as string) as person_title
            , cast(firstname as string) as person_first_name
            , cast(middlename as string) as person_middle_name
            , cast(lastname as string) as person_last_name
            , cast(suffix as string) as person_suffix
            , cast(emailpromotion as int) as person_email_promotion
            , cast(additionalcontactinfo as string) as person_additional_contact_info
            , cast(demographics as string) as person_demographics
            , cast(rowguid as string) as person_row_guid
            , cast(modifieddate as timestamp) as person_modified_date
            -- Criar nome completo
            , concat(
                coalesce(firstname, ''), 
                case when middlename is not null then ' ' || middlename else '' end,
                case when lastname is not null then ' ' || lastname else '' end
            ) as person_full_name
        from source_person
    )
    
select *
from renamed