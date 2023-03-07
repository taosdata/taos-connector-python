def tear_down_database(conn, db_name):
    conn.execute("DROP DATABASE IF EXISTS %s" % db_name)


