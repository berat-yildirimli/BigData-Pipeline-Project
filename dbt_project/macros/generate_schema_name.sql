{% macro generate_schema_name(custom_schema_name, node) -%}
    {#-
        dbt'nin varsayilan davranisi: custom_schema_name verilmisse
        "<profiles.yml default schema>_<custom_schema_name>" uretir.
        Bizim durumumuzda bu "default_silver" / "default_gold" demek --
        istedigimiz temiz "silver" / "gold" Hive database isimleri degil.
        Bu override, custom_schema_name varsa onu oldugu gibi kullanir.
    -#}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
