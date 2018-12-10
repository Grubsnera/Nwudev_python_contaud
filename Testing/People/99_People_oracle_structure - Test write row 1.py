"""
Script to extract raw data from Oracle HR System
Created on: 14/12/2017
Created by: Albert J v Rensburg (21162395)
Modified on:
"""

# Declare variables
c_dest = r'W:\Oracle_hr\Export'

import csv
import pyodbc
cnxn = pyodbc.connect("DSN=Hr;PWD=potjiekos")
curs = cnxn.cursor()

# HR.PER_JOB_DEFINITIONS

print "HR.PER_JOB_DEFINITIONS"

# Create a .csv file with the column names
f = open(c_dest+r'\column_names.csv','w')
csvf = csv.writer(f, lineterminator='\r', quoting=csv.QUOTE_NONNUMERIC)
for row in curs.execute("select column_name FROM ALL_TAB_COLUMNS WHERE table_name = 'PER_JOB_DEFINITIONS' ORDER BY column_id").fetchall():
    csvf.writerow(row)
f.close()

# Read the column names into a list variable
header_list = []
f = open(c_dest+r'\column_names.csv','rU')
reader = csv.reader(f)
for row in reader:
    header_list = header_list + row
f.close()
#print header_list

# Write the data .csv with the header first
f = open(c_dest+r'\per_job_definitions.csv','w')
csvf = csv.writer(f, lineterminator='\r', quoting=csv.QUOTE_NONNUMERIC)
csvf.writerow(header_list)
for row in curs.execute('select * from HR.PER_JOB_DEFINITIONS').fetchall():
    csvf.writerow(row)
f.close()
