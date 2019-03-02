/*

with
cte_source as
(
select  {hash business key(s)} as {Hash Key}
        , dbt.dv.utils(context_columns) --- all columns except {{ business key(s), {{[metadata columns]}}}}
        , dbt.dv.utils(hashdiff) as {{ hashdiff }}
from    {{ref(base/source model)}} src

{% if is_incremental() %}

where   not exists (
                    select  1
                    from    {{ this }} trg
                    where   trg.{Hash Key} = {hash business key(s)}
                    and     trg.{{Load Date}} = src.{{Load Date}}
                    )


{% endif %}
),
cte_condensed as
(
select  hash business key(s),
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
),
{% if is_incremental() %}
,cte_target as
(
select  trg.{Hash Key},
        trg.{{ hashdiff }}
from    {{ this }} trg
        inner join (
                    select  current_rows.{Hash Key},
                            max(load_dte) as load_dte
                    from    {{this}} current_rows
                    group by
                            current_rows.{Hash Key},
                  ) cr on trg.{Hash Key}, = cr.{Hash Key}, and trg.{{Load Date}}  = cr.{{Load Date}}
)
{% endif %}
select  src.{Hash Key}
        , dbt.dv.utils(context_columns)
        , src.{{ hashdiff }}
        , min(src.load_dte) as {{Load Date}}
        ', {{ invocation_id }}' as {load audit column name}
from    cte_condensed src

{% if is_incremental() %}

        left join cte_target trg on src.{Hash Key} = trg.{Hash Key} and src.{{ hashdiff }} = trg.{{ hashdiff }}

where   trg.{Hash Key} is null

{% endif %}

group by
        src.{Hash Key}
        , dbt.dv.utils(star)
        , src.diff_hsh
        , src.dbt_dv_utils_ts

*/
