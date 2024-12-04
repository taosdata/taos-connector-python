import os

import pytest
from sqlalchemy import create_engine, inspect

URL = "taosrest://root:taosdata@localhost:6041/test"

@pytest.fixture(scope="module")
def engine():
  return create_engine(URL)

@pytest.fixture(scope="module")
def connection(engine):
  return engine.connect()

@pytest.fixture(scope="module")
def inspection(engine):
  return inspect(engine)

@pytest.fixture(scope="module")  
def database(connection):
  # create a database for testing
  connection.execute("DROP DATABASE IF EXISTS test")
  connection.execute("CREATE DATABASE test")
  connection.execute("CREATE STABLE test.stb (ts TIMESTAMP, c1 INT, c2 DOUBLE) TAGS(location TINYINT)")
  connection.execute("CREATE TABLE test.tb USING test.stb TAGS(1)")
  connection.execute("INSERT INTO test.tb VALUES (now, -100, -200.3) (now+10s, -101, -340.2423424)")
  
  yield "database"
  
  # drop the database after testing
  connection.execute("DROP DATABASE IF EXISTS test")
  
def test_get_schema_names(database, inspection):
  schema_names = inspection.get_schema_names()
  assert schema_names
  assert "test" in schema_names
  
def test_has_table(inspection):
  has_table = inspection.has_table("stb", schema = "test")
  assert has_table
  
def test_get_table_names(inspection):
  table_names = inspection.get_table_names(schema = "test")
  assert table_names
  assert "stb" in table_names

def test_get_view_names(inspection):
  view_names = inspection.get_view_names()
  assert not view_names
  
def test_get_columns(inspection):
  columns = inspection.get_columns("stb", schema = "test")
  assert columns
  assert len(columns) == 4

def test_get_pk_constraint(inspection):
  pk_constraint = inspection.get_pk_constraint("stb", schema = "test")
  assert pk_constraint
  assert pk_constraint["constrained_columns"] == ["ts"]

def test_get_foreign_keys(inspection):
  foreign_keys = inspection.get_foreign_keys("stb", schema = "test")
  assert not foreign_keys
  
def test_get_indexes(inspection):
  indexes = inspection.get_indexes("stb", schema = "test")
  assert indexes
  assert len(indexes) == 1
  assert indexes[0]["name"] == "location_stb"
  assert indexes[0]["column_names"] == ["location"]
  
def test_select(connection):
  rows = connection.execute("SELECT * FROM test.stb").fetchall()
  assert rows
  assert len(rows) == 2
