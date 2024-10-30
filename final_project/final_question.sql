SELECT eup.state, count(s.client_id) sales_count FROM `de-07-denys-kondratiuk.silver.sales` s
  JOIN `de-07-denys-kondratiuk.gold.enrich_user_profiles` as eup
    ON eup.client_id = s.client_id
  WHERE s.product_name = "TV"
    AND s.purchase_date BETWEEN DATE("2022-09-01") AND DATE("2022-09-11")
    AND DATE_DIFF(CURRENT_DATE(), eup.birth_date, YEAR) BETWEEN 20 AND 30
  GROUP BY eup.state
  ORDER BY sales_count
