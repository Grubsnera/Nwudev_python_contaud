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

# ALL_TABLES
f = open(c_dest+r'\all_tables.csv','w')
csvf = csv.writer(f, lineterminator='\r', quoting=csv.QUOTE_NONNUMERIC)
fnames = ['OWNER', 'TABLE_NAME', 'TABLESPACE_NAME', 'NUM_ROWS', 'AVG_ROW_LEN', 'LAST_ANALYZED', 'TEMPORARY', 'SECONDARY', 'NESTED', 'MONITORING']
csvf.writerow(fnames)
for row in curs.execute('select owner, table_name, tablespace_name, num_rows, avg_row_len, last_analyzed, temporary, secondary, nested, monitoring from ALL_TABLES').fetchall():
    csvf.writerow(row)
f.close()

# ALL_COLUMNS
f = open(c_dest+r'\all_columns.csv','w')
csvf = csv.writer(f, lineterminator='\r', quoting=csv.QUOTE_NONNUMERIC)
fnames = ['OWNER', 'TABLE_NAME', 'COLUMN_NAME', 'DATA_TYPE', 'DATA_LENGTH', 'NULLABLE', 'COLUMN_ID', 'DEFAULT_LENGTH', 'DATA_DEFAULT']
csvf.writerow(fnames)
for row in curs.execute('select owner, table_name, column_name, data_type, data_length, nullable, column_id, default_length, data_default from ALL_TAB_COLUMNS').fetchall():
    csvf.writerow(row)
f.close()




