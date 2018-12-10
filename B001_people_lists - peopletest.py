""" Script to build standard PEOPLE lists
Created on: 12 Apr 2018
Author: Albert J v Rensburg (NWU21162395)
"""

# Import python modules

import sys

# Add own module path
sys.path.append('S:/_my_modules')

# Import python objects
import csv
import pyodbc
import datetime
import sqlite3    

# Import own modules
import funcdate
import funccsv
import funcfile
import funcpeople
import funcmail
import funcmysql
import funcstr
import funcmysql

# Script log file
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: B001_PEOPLE_LISTS")
funcfile.writelog("-------------------------")
print("-----------------")    
print("B001_PEOPLE_LISTS")
print("-----------------")
ilog_severity = 1

# SQLITE Declare variables 
so_path = "W:/" #Source database path
re_path = "R:/People/"
so_file = "People.sqlite" #Source database
s_sql = "" #SQL statements
l_export = False
l_mail = True

# Open the SQLITE SOURCE file
with sqlite3.connect(so_path+so_file) as so_conn:
    so_curs = so_conn.cursor()
funcfile.writelog("%t OPEN SQLITE DATABASE: PEOPLE.SQLITE")

# Open the MYSQL DESTINATION table
s_database = "Web_ia_nwu"
ms_cnxn = funcmysql.mysql_open(s_database)
ms_curs = ms_cnxn.cursor()
funcfile.writelog("%t OPEN MYSQL DATABASE: " + s_database)




# Create MYSQL PEOPLE STRUCT TO WEB table *****************************************
print("Build mysql current people structure...")
ms_curs.execute("DROP TABLE IF EXISTS ia_people_struct")
funcfile.writelog("%t DROPPED MYSQL TABLE: PEOPLE_STRUCT (ia_people_struct)")
#ia_find_1_auto INT(11) NOT NULL AUTO_INCREMENT,
s_sql = """
CREATE TABLE IF NOT EXISTS ia_people_struct (
ia_find_auto INT(11) NOT NULL,
struct_employee_one VARCHAR(20),
struct_name_list_one TEXT,
struct_known_name_one TEXT,
struct_position_full_one TEXT,
struct_location_description_one TEXT,
struct_division_one TEXT,
struct_faculty_one TEXT,
struct_email_address_one TEXT,
struct_phone_work_one TEXT,
struct_phone_mobi_one TEXT,
struct_phone_home_one TEXT,
struct_grade_calc_one TEXT,
struct_employee_two VARCHAR(20),
struct_name_list_two TEXT,
struct_known_name_two TEXT,
struct_position_full_two TEXT,
struct_location_description_two TEXT,
struct_division_two TEXT,
struct_faculty_two TEXT,
struct_email_address_two TEXT,
struct_phone_work_two TEXT,
struct_phone_mobi_two TEXT,
struct_phone_home_two TEXT,
struct_grade_calc_two TEXT,
struct_employee_three VARCHAR(20),
struct_name_list_three TEXT,
struct_known_name_three TEXT,
struct_position_full_three TEXT,
struct_location_description_three TEXT,
struct_division_three TEXT,
struct_faculty_three TEXT,
struct_email_address_three TEXT,
struct_phone_work_three TEXT,
struct_phone_mobi_three TEXT,
struct_phone_home_three TEXT,
struct_grade_calc_three TEXT,
PRIMARY KEY (struct_employee_one)
)
ENGINE = InnoDB
CHARSET=utf8mb4
COLLATE utf8mb4_unicode_ci
COMMENT = 'Table to store detailed people structure data'
""" + ";"
ms_curs.execute(s_sql)
funcfile.writelog("%t CREATED MYSQL TABLE: PEOPLE_STRUCT (ia_people_struct)")
# Open the SOURCE file to obtain column headings
print("Build mysql current people structure columns...")
funcfile.writelog("%t OPEN DATABASE: People org structure")
s_head = funcmysql.get_colnames_sqlite_text(so_curs,"X003_PEOPLE_ORGA_REF","struct_")
s_head = "(`ia_find_auto`, " + s_head.rstrip(", ") + ")"
#print(s_head)
# Open the SOURCE file to obtain the data
print("Insert mysql current people structure...")
with sqlite3.connect(so_path+so_file) as rs_conn:
    rs_conn.row_factory = sqlite3.Row
rs_curs = rs_conn.cursor()
rs_curs.execute("SELECT * FROM X003_PEOPLE_ORGA_REF")
rows = rs_curs.fetchall()
i_tota = 0
i_coun = 0
for row in rows:
    s_data = "(4, "
    for member in row:
        if type(member) == str:
            s_data = s_data + "'" + member + "', "
        elif type(member) == int:
            s_data = s_data + str(member) + ", "
        else:
            s_data = s_data + "'', "
    s_data = s_data.rstrip(", ") + ")"
    #print(s_data)
    s_sql = "INSERT INTO `ia_people_struct` " + s_head + " VALUES " + s_data + ";"
    ms_curs.execute(s_sql)
    i_tota = i_tota + 1
    i_coun = i_coun + 1
    if i_coun == 100:
        print(i_coun)
        ms_cnxn.commit()
        i_coun = 0
# Close the ROW Connection
ms_cnxn.commit()
rs_conn.close()
print("Inserted " + str(i_tota) + " mysql current people...")
funcfile.writelog("%t POPULATE MYSQL TABLE: PEOPLE (ia_people) " + str(i_tota) + " records")





        
# Close the ROW Connection
ms_cnxn.commit()

# Close the connection *********************************************************
so_conn.close()
ms_cnxn.close()


# Close the log writer *********************************************************
funcfile.writelog("----------------------------")
funcfile.writelog("COMPLETED: B001_PEOPLE_LISTS")


