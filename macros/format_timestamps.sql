-- macro that formats a timestamp to a date key that references the DIM_DATE dimension table
{% macro to_date_key(_timestamp_column) %}
    {% set output = "strftime(" ~ _timestamp_column ~ ", '%Y%m%d')" %}

    {{ return(output) }}
{% endmacro %}

-- macro that formats a timestamp to a time key that references the DIM_TIME dimension table
{% macro to_time_key(_timestamp_column) %}
    {% set output = "strftime(" ~ _timestamp_column ~ ", '%H%M')" %}

    {{ return(output) }}
{% endmacro %}

-- macro that formats a timestamp to the ISO8601 date format
{% macro to_iso_date(_timestamp_column) %}
    {% set output = "strftime(" ~ _timestamp_column ~ ", '%Y-%m-%d')" %}

    {{ return(output) }}
{% endmacro %}

-- macro that formats a timestamp to the ISO8601 date format but with dots delimiters instead of dashes
{% macro to_iso_date_us(_timestamp_column) %}
    {% set output = "strftime(" ~ _timestamp_column ~ ", '%Y.%m.%d')" %}

    {{ return(output) }}
{% endmacro %}

-- macro that formats a timestamp to British date
{% macro to_date_gb(_timestamp_column) %}
    {% set output = "strftime(" ~ _timestamp_column ~ ", '%d/%m/%Y')" %}

    {{ return(output) }}
{% endmacro %}

-- macro that formats a timestamp to US date
{% macro to_date_us(_timestamp_column) %}
    {% set output = "strftime(" ~ _timestamp_column ~ ", '%m/%d/%Y')" %}

    {{ return(output) }}
{% endmacro %}

-- macro that returns the current age from a time
{% macro to_age(_timestamp_column) %}
    {% set output = "(year(current_date) - year(" ~ _timestamp_column ~ "))" %}

    {{ return(output) }}
{% endmacro %}