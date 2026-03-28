{% macro get_rfm_segment_description(segment_name) %}
  CASE
    {{ segment_name }}

    WHEN 'CHAMPIONS' THEN 'Best customers: High recency, frequency, and monetary value (R≥4, F≥4, M≥4)'

    WHEN 'LOYAL_CUSTOMERS' THEN 'Highly frequent customers with good recency (R≥3, F≥4)'

    WHEN 'POTENTIAL_LOYALIST' THEN 'Customers with strong engagement across all metrics (R≥3, F≥3, M≥3)'

    WHEN 'NEW_CUSTOMERS' THEN 'Recent customers with low purchase frequency (R≥4, F≤2)'

    WHEN 'PROMISING' THEN 'Recent customers showing growing engagement (R≥3, F≥2, M≥2)'

    WHEN 'NEED_ATTENTION' THEN 'Average customers needing engagement (R≥3, F≤3, M≤3)'

    WHEN 'CANNOT_LOSE_THEM' THEN 'High-value customers with low recency, at high risk of churn (R≤2, F≥4, M≥4)'

    WHEN 'AT_RISK' THEN 'Previously valuable customers becoming inactive (R≤2, F≥3 OR M≥3)'

    WHEN 'HIBERNATING' THEN 'Inactive customers with moderate past engagement (R≤2, F≥2, M≥2)'

    WHEN 'LOST' THEN 'Customers with low recency, frequency, and monetary value (R≤2, F≤2, M≤2)'

    WHEN 'BIG_SPENDERS' THEN 'Customers with high monetary value but low frequency purchases (M≥4, F≤2)'

    WHEN 'STANDARD' THEN 'Customers with mixed or average engagement patterns'

    ELSE 'Unclassified segment'
  END
{% endmacro %}
