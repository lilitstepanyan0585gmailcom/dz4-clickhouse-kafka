import clickhouse_connect

def main():
    client = clickhouse_connect.get_client(host='localhost', port=8123)
    query = """
    SELECT
      us_state,
      argMax(cat_id, amount) AS cat_id_of_max_amount,
      max(amount) AS max_amount
    FROM transactions_db.transactions
    GROUP BY us_state
    ORDER BY us_state
    """
    df = client.query_df(query)
    df.to_csv("result.csv", index=False)
    print("Saved result.csv")

if __name__ == "__main__":
    main()
