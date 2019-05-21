
import sys

# Add own module path
sys.path.append('S:/_my_modules')
sys.path.append('S:/')

import funcdate
import funcfile
import funcmail
import funcsys

funcfile.writelog("Now")
funcfile.writelog("SCRIPT: A001_ORACLE_TO_SQLITE_RUN_LARGE")
funcfile.writelog("---------------------------------------")

if funcdate.today_dayname() in "MonTueWedThuFri":
    import A001_oracle_to_sqlite
    # Environment
    l_mail = True
    # Extract data from oracle
    try:
        A001_oracle_to_sqlite.Oracle_to_sqlite("000b_Table - large.csv")
    except Exception as e:
        funcsys.ErrMessage(e)
else:
    print("ORACLE to SQLITE LARGE do not run on Saturdays and Sundays")
    funcfile.writelog("SCRIPT: A001_ORACLE_TO_SQLITE_LARGE: DO NOT RUN ON SATURDAYS AND SUNDAYS")

# Close the log writer
funcfile.writelog("Now")
funcfile.writelog("COMPLETED: A001_ORACLE_TO_SQLITE_RUN_LARGE")
funcfile.writelog("------------------------------------------")
