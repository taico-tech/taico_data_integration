{% macro insert_data(source_project_id, source_dataset_id, source_table, target_project_id, target_dataset_id, target_table) %}
  {% set sql %}
    INSERT INTO `{{ target_project_id }}.{{ target_dataset_id }}.{{ target_table }}`
    SELECT * FROM `{{ source_project_id }}.{{ source_dataset_id }}.{{ source_table }}`
  {% endset %}
  {{ return(sql) }}
{% endmacro %}
