"""
Script to obtain large databases from oracle
Author: Albert Janse van Rensburg
Created: 5 Jul 2019
"""

# IMPORT OWN MODULES
from _my_modules import funcdate
from _my_modules import funcfile
from _my_modules import funcmail
from _my_modules import funcsys

# DECLARE VARIABLES
l_mail = True

if l_mail:
    funcmail.Mail('std_success_gmail', 'Python:Success:Start A001_oracle_to_sqlite_large', 'NWUIAPython: Success: Start A001_oracle_to_sqlite_large')

funcfile.writelog("Now")
funcfile.writelog("SCRIPT: A001_ORACLE_TO_SQLITE_RUN_LARGE")
funcfile.writelog("---------------------------------------")

if funcdate.today_dayname() in "MonTueWedThuFri":
    import A001_oracle_to_sqlite
    try:
        A001_oracle_to_sqlite.Oracle_to_sqlite("000b_Table - large.csv")
    except Exception as e:
        funcsys.ErrMessage(e, True, 'NWUIAPython:Fail:A001_oracle_to_sqlite_large', 'NWUIAPython: Fail: A001_oracle_to_sqlite_large')
else:
    print("ORACLE to SQLITE LARGE do not run on Saturdays and Sundays")
    funcfile.writelog("SCRIPT: A001_ORACLE_TO_SQLITE_LARGE: DO NOT RUN ON SATURDAYS AND SUNDAYS")

# Close the log writer
funcfile.writelog("Now")
funcfile.writelog("COMPLETED: A001_ORACLE_TO_SQLITE_RUN_LARGE")
funcfile.writelog("------------------------------------------")

# SEND MAIL TO INDICATE THE SUCCESSFUL COMPLETION OF ALL PYTHON SCRIPTS
if l_mail:
    funcmail.Mail("python_log")

