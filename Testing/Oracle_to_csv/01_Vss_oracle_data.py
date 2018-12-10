"""
Script to extract raw data from Oracle HR System
Created on: 25 Jan 2018
Created by: Albert J v Rensburg (21162395)
Modified on:
"""

# Import python objects
import csv
import pyodbc
import sqlite3

# Define Functions

def write_header(o_odbc, c_whd, c_wht):
    
    # Write a column header file
    
    """
    Parameter
        whf = The name of the table for which to obtain header record
    """
    
    f = open(c_whd + "/COLUMN_NAMES.csv", "w")
    csvf = csv.writer(f, lineterminator='\r', quoting=csv.QUOTE_NONNUMERIC)
    for row in o_odbc.execute("select column_name FROM ALL_TAB_COLUMNS WHERE table_name = '" + c_wht + "' ORDER BY column_id").fetchall():
        csvf.writerow(row)
    f.close()

def read_header(c_rhd):
    
    # Read the columns from the column header file
    
    header_list = []
    f = open(c_rhd + "/COLUMN_NAMES.csv", "rU")
    reader = csv.reader(f)
    for row in reader:
        header_list = header_list + row
    f.close()
    return header_list

def write_data(o_odbc, c_wdd, c_wdo, c_wdt, c_wdh):
    
    # Extract and read the data
    
    """
    Parameter
        wdc = ODBC Cursor
        wdd = Data file destination
        wdf = Data file name
        wdt = Data table name
        wdh = Data table header record
    """
    
    f = open(c_wdd + "/" + c_wdt + ".csv", "w")
    csvf = csv.writer(f, lineterminator='\r', quoting=csv.QUOTE_NONNUMERIC)
    csvf.writerow(c_wdh)
    for row in o_odbc.execute("select * from " + c_wdo + "." + c_wdt ).fetchall():
        csvf.writerow(row)
    f.close()

# Create the local SQLite3 database
with sqlite3.connect("Vss.db") as con:
     c = con.cursor()

# Connect to the oracle database
cnxn = pyodbc.connect("DSN=Vss;PWD=potjiekos")
curs = cnxn.cursor()

# Declare variables
c_dest = "W:/Vss_stud_acc/Raw"

# Read the table info for which data should met extracted

tabcsvf = open("01_Vss_oracle_data.csv", "rU")
readert = csv.reader(tabcsvf)
for row in readert:

    c_owne = row[0]
    c_tabl = row[1]

    print(c_tabl)

    # Create a .csv file with the column names
    write_header(curs, c_dest, c_tabl)

    # Read the column names into a list variable
    c_head = read_header(c_dest)
    #print c_head

    # Write the data .csv with the header first
    write_data(curs, c_dest,c_owne,c_tabl,c_head)
        
tabcsvf.close()


