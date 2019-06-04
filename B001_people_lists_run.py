
import sys

# Add own module path
sys.path.append('S:/_my_modules')
sys.path.append('S:/')

import funcfile
import funcmail
import funcsys

funcfile.writelog("Now")
funcfile.writelog("SCRIPT: B001_RUN_PEOPLE")
funcfile.writelog("-----------------------")

import B001_people_lists

# Environment
l_mail = True

# People lists
try:
    B001_people_lists.people_lists()
except Exception as e:
    funcsys.ErrMessage(e)

# Close the log writer
funcfile.writelog("Now")
funcfile.writelog("COMPLETED: B001_RUN_PEOPLE")
funcfile.writelog("--------------------------")
