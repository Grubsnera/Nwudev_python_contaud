# IMPORT OWN MODULES
from _my_modules import funcfile
from _my_modules import funcsys

# IMPORT THE MODULE
import C003_people_list_masterfile

# OPEN THE LOG
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C003_PEOPLE_LIST_MASTERFILE_RUN")
funcfile.writelog("---------------------------------------")

# PEOPLE LISTS
try:
    C003_people_list_masterfile.people_list_masterfile()
except Exception as e:
    funcsys.ErrMessage(e)

# CLOSE THE LOG
funcfile.writelog("Now")
funcfile.writelog("COMPLETED: C003_PEOPLE_LIST_MASTERFILE_RUN")
funcfile.writelog("------------------------------------------")
