
import sys

# Add own module path
sys.path.append('S:/_my_modules')
sys.path.append('S:/')

import funcfile
import funcmail
import funcsys

funcfile.writelog("Now")
funcfile.writelog("SCRIPT: B005_RUN_MYSQL")
funcfile.writelog("----------------------")

import B005_mysql_lists

# Environment
l_mail = True

# People lists
try:
    B005_mysql_lists.mysql_lists()
except Exception as e:
    funcsys.ErrMessage(e)

# Close the log writer
funcfile.writelog("Now")
funcfile.writelog("COMPLETED: B005_RUN_MYSQL")
funcfile.writelog("-------------------------")
