{#
select  {Hash Key},
        , dbt.dv.utils(context_columns)
        {{ hashdiff }},
        {{Load Date}} ,
        row_number() over (partition by {Hash Key} order by {{Load Date}} )
        -
        row_number() over (partition by
                                        {Hash Key},
                                        {{ hashdiff }}
                                        order by {{Load Date}} ) as dbt_dv_utils_ts
from    cte_source src
group by
        src.{Hash Key}
        , dbt.dv.utils(star)
        , src.diff_hsh
        , src.dbt_dv_utils_ts
#}
