from _my_modules import funcfile
from _my_modules import funcsys

funcfile.writelog("Now")
funcfile.writelog("SCRIPT: A001_ORACLE_TO_SQLITE_RUN")
funcfile.writelog("---------------------------------")

import A001_oracle_to_sqlite

# Extract data from oracle
try:
    A001_oracle_to_sqlite.oracle_to_sqlite("000b_Table - temp.csv")
except Exception as e:
    funcsys.ErrMessage(e)

# Close the log writer
funcfile.writelog("Now")
funcfile.writelog("COMPLETED: A001_ORACLE_TO_SQLITE_RUN")
funcfile.writelog("------------------------------------")
