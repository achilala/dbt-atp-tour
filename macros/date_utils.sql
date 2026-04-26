-- Returns an integer date key in YYYYMMDD format for joining to dim_date
{% macro to_date_key(timestamp_column) %}
    {% set _result = "strftime(" ~ timestamp_column ~ ", '%Y%m%d')" %}
    {{ return(_result) }}
{% endmacro %}

-- Returns an integer time key in HHMM format for joining to dim_time
{% macro to_time_key(timestamp_column) %}
    {% set _result = "strftime(" ~ timestamp_column ~ ", '%H%M')" %}
    {{ return(_result) }}
{% endmacro %}

-- Returns a TIME value from an integer minute offset (0–1439)
{% macro to_minutes(minute_num) %}
    {% set _result = "make_time(floor(" ~ minute_num ~ " / 60)::int, (" ~ minute_num ~ " % 60)::int, 0)" %}
    {{ return(_result) }}
{% endmacro %}

-- Returns date as ISO 8601 string: YYYY-MM-DD
{% macro to_iso_date(timestamp_column) %}
    {% set _result = "strftime(" ~ timestamp_column ~ ", '%Y-%m-%d')" %}
    {{ return(_result) }}
{% endmacro %}

-- Returns date with dot delimiters: YYYY.MM.DD
{% macro to_iso_date_us(timestamp_column) %}
    {% set _result = "strftime(" ~ timestamp_column ~ ", '%Y.%m.%d')" %}
    {{ return(_result) }}
{% endmacro %}

-- Returns date in British format: DD/MM/YYYY
{% macro to_date_gb(timestamp_column) %}
    {% set _result = "strftime(" ~ timestamp_column ~ ", '%d/%m/%Y')" %}
    {{ return(_result) }}
{% endmacro %}

-- Returns date in US format: MM/DD/YYYY
{% macro to_date_us(timestamp_column) %}
    {% set _result = "strftime(" ~ timestamp_column ~ ", '%m/%d/%Y')" %}
    {{ return(_result) }}
{% endmacro %}

-- Returns age in whole years as of the current date
{% macro to_age(timestamp_column) %}
    {% set _result = "extract(year from age(current_date, " ~ timestamp_column ~ "))" %}
    {{ return(_result) }}
{% endmacro %}
