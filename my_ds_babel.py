import pandas as pd
import sqlite3 as sql


def csv_to_sql(csv_content, data, table):
    
    df = pd.read_csv(csv_content)
    con = sql.connect(data)

    col_str = ','.join('"{}"'.format(col.replace('_', ' ')) for col in df.columns)

    query = f"CREATE TABLE IF NOT EXISTS {table} ({col_str})"
    con.execute(query)

    for row in df.itertuples(index=False):
        query = f"INSERT INTO {table} VALUES ({','.join('?' * len(row))})"
        con.execute(query, row)

    con.commit()

    con.close()


