{% macro get_rfm_segment_description(segment_name) %}
  CASE
    {{ segment_name }}
    WHEN 'CHAMPIONS' THEN 'Best customers: High recency, frequency, and monetary value (R≥4, F≥4, M≥4)'
    WHEN 'LOYAL_CUSTOMERS' THEN 'Frequent buyers with consistent purchase patterns (F≥4)'
    WHEN 'POTENTIAL_LOYALIST' THEN 'Promising customers with good overall scores (R≥3, F≥3, M≥3)'
    WHEN 'NEW_CUSTOMERS' THEN 'Recent first-time or low-frequency buyers (R≥4, F≤2)'
    WHEN 'PROMISING' THEN 'Recent customers starting to engage more (R≥3, F≥2, M≥2)'
    WHEN 'NEED_ATTENTION' THEN 'Average customers requiring nurturing (R≥3, F≤3, M≤3)'
    WHEN 'AT_RISK' THEN 'Previously good customers becoming inactive (R≤2, F≥3 or M≥3)'
    WHEN 'CANNOT_LOSE_THEM' THEN 'High-value customers at risk of churn (R≤2, F≥4, M≥4)'
    WHEN 'HIBERNATING' THEN 'Former active customers now dormant (R≤2, F≥2, M≥2)'
    WHEN 'LOST' THEN 'Churned customers with low engagement (R≤2, F≤2, M≤2)'
    WHEN 'BIG_SPENDERS' THEN 'High monetary value but infrequent purchases (M≥4, F≤2)'
    WHEN 'STANDARD' THEN 'Standard customers with mixed engagement patterns'
    ELSE 'Unclassified segment'
  END
{% endmacro %}
