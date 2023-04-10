/*
Custom schema logic
*/

{% macro generate_schema_name(custom_schema_name, node) -%}
    
    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}        
        {{ default_schema }}
    {%- elif default_schema == "" -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}