{% macro grant_permissions(role) %}
  {% if target.name == 'prod' %}
    {% set sql %}
      GRANT SELECT ON ALL TABLES IN SCHEMA {{ schema }} TO {{ role }};
      GRANT USAGE ON SCHEMA {{ schema }} TO {{ role }};
    {% endset %}
    {{ log("Granting permissions to role: " ~ role, info=true) }}
    {{ run_query(sql) }}
  {% endif %}
{% endmacro %}