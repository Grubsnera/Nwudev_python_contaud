
import sys

# OWN MODULE PATH
sys.path.append('S:/_my_modules')
sys.path.append('S:/')

# IMPORT MODULES
import funcfile
import funcsys

# OPEN THE LOG WRITER
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C100_PEOPLE_TEST_MASTERFILE_RUN")
funcfile.writelog("---------------------------------------")

import C001_people_test_masterfile
try:
    C001_people_test_masterfile.People_test_masterfile()
except Exception as e:
    funcsys.ErrMessage(e)

# CLOSE THE LOG WRITER
funcfile.writelog("Now")
funcfile.writelog("COMPLETED: C100_PEOPLE_TEST_MASTERFILE_RUN")
funcfile.writelog("------------------------------------------")

