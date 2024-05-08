{% macro prefix_fields(alias, fields) %}
     {% set prefixed_fields = [] %}
     {% for field in fields %}
         {{ prefixed_fields.append(alias ~ '.' ~ field) }}
     {% endfor %}
     {{ return(prefixed_fields | join(', ')) }}
 {% endmacro %}