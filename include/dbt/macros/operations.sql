{% macro truncate_table(project_id, dataset_id, table_name) %}
  {% set sql %}
    TRUNCATE TABLE `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  {% endset %}
  {{ return(sql) }}
{% endmacro %}
