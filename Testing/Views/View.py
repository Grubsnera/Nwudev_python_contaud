"""
Script to extract raw data from an ODBC data source
Created on: 10 Feb 2018
Copyright: Albert J v Rensburg
Modified on:
"""

""" Import python objects """
import pyodbc
import sqlite3

""" Declare the global variables """

de_fil = "W:/" #Destination file name
de_pat = "Vss.db" #Destination file path
de_sql = ""

""" OPEN THE DESTINATION SQlite3 file """
with sqlite3.connect(de_fil+de_pat) as de_con:
    de_cur = de_con.cursor()

de_sql = "CREATE VIEW LIST_RESIDENCE AS "\
"SELECT "\
  "main.RESIDENCE.KRESIDENCEID,"\
  "main.RESIDENCENAME.NAME,"\
  "main.RESIDENCE.FSITEORGUNITNUMBER,"\
  "main.RESIDENCE.STARTDATE,"\
  "main.RESIDENCE.ENDDATE,"\
  "main.RESIDENCE.RESIDENCECAPACITY,"\
  "main.RESIDENCE.FRESIDENCETYPECODEID,"\
  "main.CODEDESCRIPTION.CODESHORTDESCRIPTION "\
"FROM "\
  "main.RESIDENCE "\
  "LEFT JOIN main.RESIDENCENAME ON main.RESIDENCENAME.KRESIDENCEID = main.RESIDENCE.KRESIDENCEID "\
  "LEFT JOIN main.CODEDESCRIPTION ON main.CODEDESCRIPTION.KCODEDESCID = main.RESIDENCE.FRESIDENCETYPECODEID "\
"WHERE "\
  "main.RESIDENCENAME.KSYSTEMLANGUAGECODEID = 3 AND "\
  "main.CODEDESCRIPTION.KSYSTEMLANGUAGECODEID = 3 "\
"ORDER BY "\
  "main.RESIDENCE.STARTDATE;"
de_cur.execute("DROP VIEW LIST_RESIDENCE")
de_cur.execute(de_sql)
de_con.commit()
de_con.close()
