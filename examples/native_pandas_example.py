# ANCHOR: connect
import pandas
from sqlalchemy import create_engine, text

def connect():
    """Create a connection to TDengine using SQLAlchemy"""
    engine = create_engine(f"taos://root:taosdata@localhost:6030?timezone=Asia/Shanghai")
    conn = engine.connect()
    print("Connected to TDengine successfully.")
    return conn


def pandas_to_sql_example(conn):
    """Test writing data to TDengine using pandas DataFrame.to_sql() method and verify the results"""
    from sqlalchemy.types import Integer, Float, TIMESTAMP, String
    # Prepare test data
    data = {
        "ts": [1626861392589, 1626861392590, 1626861392591],
        "current": [11.5, 12.3, 13.7],
        "voltage": [220, 230, 240],
        "phase": [1.0, 1.1, 1.2],
        "location": ["california.losangeles", "california.sandiego", "california.sanfrancisco"],
        "groupid": [2, 2, 3],
        "tbname": ["california", "sandiego", "xxxx"]
    }
    df = pandas.DataFrame(data)

    try:
        # 1. Create table structure first
        conn.execute(text("CREATE DATABASE IF NOT EXISTS power"))
        conn.execute(text(
            "CREATE STABLE IF NOT EXISTS power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)"))
        conn.execute(text("Use power"))

        # 3. Insert data using pandas to_sql
        data = {
            "ts": [1626861392589, 1626861392590, 1626861392591],
            "current": [11.5, 12.3, 13.7],
            "voltage": [220, 230, 240],
            "phase": [1.0, 1.1, 1.2],
            "location": ["california.losangeles", "california.sandiego", "california.sanfrancisco"],
            "groupid": [2, 2, 3],
            "tbname": ["california", "sandiego", "xxxx"]
        }
        df = pandas.DataFrame(data)
        rows_affected = df.to_sql("meters", conn, if_exists="append", index=False,
                                  dtype={
                                      "ts": TIMESTAMP,
                                      "current": Float,
                                      "voltage": Integer,
                                      "phase": Float,
                                      "location": String,
                                      "groupid": Integer,
                                  })
        assert rows_affected == 3, f"Expected to insert 3 rows, affected {rows_affected} rows"
    except Exception as err:
        print(f"Failed to insert data into power.meters, ErrMessage:{err}")
        raise err


def pandas_read_sql_example(conn):
    """Test reading data from TDengine using pandas read_sql() method"""
    try:
        df = pandas.read_sql(text("SELECT * FROM power.meters"), conn, parse_dates=["ts"])
        print(df.head(3))
        print("Read data from TDengine successfully.")
    except Exception as err:
        print(f"Failed to read data from power.meters, ErrMessage:{err}")
        raise err


def pandas_read_sql_table_example(conn):
    """Test reading data from TDengine using pandas read_sql_table() method"""
    try:
        df = pandas.read_sql_table(
            table_name='meters',
            con=conn,
            index_col='ts',
            parse_dates=['ts'],
            columns=[
                'ts',
                'current',
                'voltage',
                'phase',
                'location',
                'groupid'
            ],
        )
        print(df.head(3))
        print("Read data from TDengine successfully using read_sql_table.")
    except Exception as err:
        print(f"Failed to read data from power.meters using read_sql_table, ErrMessage:{err}")
        raise err


def test_pandas():
    conn = connect()
    pandas_to_sql_example(conn)
    pandas_read_sql_example(conn)
    pandas_read_sql_table_example(conn)
