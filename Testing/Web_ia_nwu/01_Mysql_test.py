"""
Script to test various MYSQL Functions
Created on: 12 Oct 2018
Created by: Albert J v Rensburg (21162395)
Modified on:
"""

import sys

# Add own module path
sys.path.append('S:/_my_modules')

# Import python objects
import csv
import pyodbc
import datetime
import sqlite3

# Define Functions
import funcfile
import funcstr
import funcdate
import funccsv
import funcpeople
import funcmail
import funcmysql

# Declare variables
so_path = "W:/" #Source database path
re_path = "R:/People/"
so_file = "People.sqlite" #Source database
s_sql = "" #SQL statements
l_export = True
l_mail = True

# Connect to the oracle database
cnxn = funcmysql.mysql_open("Web_ia_nwu")
curs = cnxn.cursor()

# Open the SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("%t OPEN DATABASE: Kfs")

# Attach data sources
so_curs.execute("ATTACH DATABASE 'W:/People.sqlite' AS 'PEOPLE'")
funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

s_sql = """
CREATE TABLE ia_people_birthday (
SELECT
  PEOPLE.X002_PEOPLE_CURR.EMPLOYEE_NUMBER,
  PEOPLE.X002_PEOPLE_CURR.DATE_OF_BIRTH,
  PEOPLE.X002_PEOPLE_CURR.NAME_LIST,
  PEOPLE.X002_PEOPLE_CURR.KNOWN_NAME,
  PEOPLE.X002_PEOPLE_CURR.POSITION_FULL,
  PEOPLE.X002_PEOPLE_CURR.OE_CODE
FROM
  PEOPLE.X002_PEOPLE_CURR
)
""" + ";"
curs.execute("DROP TABLE IF EXISTS ia_people_birthday")
curs.execute(s_sql)
cnxn.commit()

"""
WHERE
  StrfTime('%m-%d', PEOPLE.X002_PEOPLE_CURR.DATE_OF_BIRTH) >= StrfTime('%m-%d', 'now') AND
  StrfTime('%m-%d', PEOPLE.X002_PEOPLE_CURR.DATE_OF_BIRTH) <= StrfTime('%m-%d', 'now', '+7 day')
ORDER BY
  StrfTime('%m-%d', PEOPLE.X002_PEOPLE_CURR.DATE_OF_BIRTH),
  PEOPLE.X002_PEOPLE_CURR.POSITION_FULL
"""



