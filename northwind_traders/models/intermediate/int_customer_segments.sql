{{ config(
    materialized='table',
    schema='intermediate',
    tags=['intermediate', 'customer'],
    cluster_by=['customer_id'],
    tblproperties={
        'delta.logRetentionDuration': '7 days',
        'delta.autoOptimize.autoCompact': 'true',
        'delta.autoOptimize.optimizeWrite': 'true'
    }
) }}

WITH rfm_scores AS (
    SELECT 
        customer_id,
        recency_days,
        frequency,
        monetary,
        first_order_date,
        last_order_date,
        avg_order_value,
        customer_tenure_days,
        r_score,
        f_score,
        m_score,
        rfm_code,
        calculated_at
    FROM {{ ref('int_customer_rfm_scores') }}
),
customer_segments AS (
  SELECT 
    customer_id,
    recency_days,
    frequency,
    monetary,
    first_order_date,
    last_order_date,
    avg_order_value,
    customer_tenure_days,
    r_score,
    f_score,
    m_score,
    rfm_code,
    
    {{ calculate_rfm_segment('r_score', 'f_score', 'm_score') }} AS customer_segment,
    
    calculated_at
    
  FROM rfm_scores
)

SELECT 
  customer_id,
  recency_days,
  frequency,
  monetary,
  first_order_date,
  last_order_date,
  avg_order_value,
  customer_tenure_days,
  r_score,
  f_score,
  m_score,
  rfm_code,
  customer_segment,
  
  {{ get_rfm_segment_description('customer_segment') }} AS segment_description,
  
  calculated_at
  
FROM customer_segments