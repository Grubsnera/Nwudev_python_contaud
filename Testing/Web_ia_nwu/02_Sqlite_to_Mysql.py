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

# Open the SOURCE file to obtain column headings
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("%t OPEN DATABASE: People")
s_head = funcmysql.get_colnames_sqlite_text(so_curs,"X002_PEOPLE_CURR","people_")
s_head = "(" + s_head.rstrip(", ") + ")"
#print(s_head)

# Open the SOURCE file to obtain the data
so_conn.row_factory = sqlite3.Row
so_curs = so_conn.cursor()
so_curs.execute("SELECT * FROM X002_PEOPLE_CURR")
rows = so_curs.fetchall()
i_tota = 0
i_coun = 0
for row in rows:
    s_data = ""
    for member in row:
        if type(member) == str:
            s_data = s_data + "'" + member + "', "
        elif type(member) == int:
            s_data = s_data + str(member) + ", "
        else:
            s_data = s_data + "'', "
    s_data = "(" + s_data.rstrip(", ") + ")"
    #print(s_data)
    s_sql = "INSERT INTO `ia_people` " + s_head + " VALUES " + s_data + ";"
    curs.execute(s_sql)
    i_tota = i_tota + 1
    i_coun = i_coun + 1
    if i_coun == 100:
        cnxn.commit()
        i_coun = 0
    if i_tota == 1000:
        print(i_tota)

print(i_tota)
so_conn.close()
cnxn.commit()
cnxn.close()











# Attach data sources
#so_curs.execute("ATTACH DATABASE 'W:/People.sqlite' AS 'PEOPLE'")
#funcfile.writelog("%t ATTACH DATABASE: PEOPLE.SQLITE")

#curs.execute("DROP TABLE IF EXISTS ia_people_birthday")
#curs.execute(s_sql)
#cnxn.commit()

"""
WHERE
  StrfTime('%m-%d', PEOPLE.X002_PEOPLE_CURR.DATE_OF_BIRTH) >= StrfTime('%m-%d', 'now') AND
  StrfTime('%m-%d', PEOPLE.X002_PEOPLE_CURR.DATE_OF_BIRTH) <= StrfTime('%m-%d', 'now', '+7 day')
ORDER BY
  StrfTime('%m-%d', PEOPLE.X002_PEOPLE_CURR.DATE_OF_BIRTH),
  PEOPLE.X002_PEOPLE_CURR.POSITION_FULL
"""



