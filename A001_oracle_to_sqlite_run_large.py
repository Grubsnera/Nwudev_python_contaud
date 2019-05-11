
import sys

# Add own module path
sys.path.append('S:/_my_modules')
sys.path.append('S:/')

import funcfile
import funcmail
import funcsys

funcfile.writelog("Now")
funcfile.writelog("SCRIPT: A001_ORACLE_TO_SQLITE_RUN")
funcfile.writelog("---------------------------------")

import A001_oracle_to_sqlite

# Environment
l_mail = True

# Extract data from oracle
try:
    A001_oracle_to_sqlite.Oracle_to_sqlite("000b_Table - large.csv")
except Exception as e:
    funcsys.ErrMessage(e)

# Close the log writer
funcfile.writelog("Now")
funcfile.writelog("COMPLETED: A001_ORACLE_TO_SQLITE_RUN")
funcfile.writelog("------------------------------------")
