def tear_down_database(conn, db_name):
    conn.execute_with_req_id("DROP DATABASE IF EXISTS %s" % db_name)


