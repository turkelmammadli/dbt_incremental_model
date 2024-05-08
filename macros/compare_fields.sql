{% macro compare_fields(fields, prefix1, prefix2) %}
     {% set conditions = [] %}
     {% for field in fields %}
         {{ conditions.append(prefix1 ~ '.' ~ field ~ ' <> ' ~ prefix2 ~ '.' ~ field) }}
     {% endfor %}
     {{ return(' OR '.join(conditions)) }}
 {% endmacro %}