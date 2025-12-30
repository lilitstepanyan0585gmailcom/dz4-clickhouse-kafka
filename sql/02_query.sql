USE transactions_db;

SELECT
  us_state,
  argMax(cat_id, amount) AS cat_id_of_max_amount,
  max(amount) AS max_amount
FROM transactions
GROUP BY us_state
ORDER BY us_state;
