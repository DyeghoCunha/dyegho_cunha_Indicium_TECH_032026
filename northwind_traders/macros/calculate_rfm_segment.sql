{% macro calculate_rfm_segment(
    r_score,
    f_score,
    m_score
  ) %}
  CASE
    WHEN {{ r_score }} >= 4
    AND {{ f_score }} >= 4
    AND {{ m_score }} >= 4 THEN 'CHAMPIONS'
    WHEN {{ f_score }} >= 4 THEN 'LOYAL_CUSTOMERS'
    WHEN {{ r_score }} >= 3
    AND {{ f_score }} >= 3
    AND {{ m_score }} >= 3 THEN 'POTENTIAL_LOYALIST'
    WHEN {{ r_score }} >= 4
    AND {{ f_score }} <= 2 THEN 'NEW_CUSTOMERS'
    WHEN {{ r_score }} >= 3
    AND {{ f_score }} >= 2
    AND {{ m_score }} >= 2 THEN 'PROMISING'
    WHEN {{ r_score }} >= 3
    AND {{ f_score }} <= 3
    AND {{ m_score }} <= 3 THEN 'NEED_ATTENTION'
    WHEN {{ r_score }} <= 2
    AND (
      {{ f_score }} >= 3
      OR {{ m_score }} >= 3
    ) THEN 'AT_RISK'
    WHEN {{ r_score }} <= 2
    AND {{ f_score }} >= 4
    AND {{ m_score }} >= 4 THEN 'CANNOT_LOSE_THEM'
    WHEN {{ r_score }} <= 2
    AND {{ f_score }} >= 2
    AND {{ m_score }} >= 2 THEN 'HIBERNATING'
    WHEN {{ r_score }} <= 2
    AND {{ f_score }} <= 2
    AND {{ m_score }} <= 2 THEN 'LOST'
    WHEN {{ m_score }} >= 4
    AND {{ f_score }} <= 2 THEN 'BIG_SPENDERS'
    ELSE 'STANDARD'
  END
{% endmacro %}
