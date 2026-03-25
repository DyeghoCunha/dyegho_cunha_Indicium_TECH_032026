{% macro log_dbt_results(results) %}
  {% if execute %}
    {{ log("========== RESUMO DA EXECUÇÃO OMNIFY ==========", info=True) }}
    
    {% for res in results %}
      {% set line -%}
        Modelo: {{ res.node.name }} | Status: {{ res.status }} | Tempo: {{ res.execution_time | round(2) }}s
      {%- endset %}
      
      {{ log(line, info=True) }}
    {% endfor %}
    
    {{ log("===================================================", info=True) }}
  {% endif %}
{% endmacro %}