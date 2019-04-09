
import sys

# OWN MODULE PATH
sys.path.append('S:/_my_modules')
sys.path.append('S:/')

# IMPORT MODULES
import funcfile

# OPEN THE LOG WRITER
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: C002_PEOPLE_TEST_CONFLICT_RUN")
funcfile.writelog("-------------------------------------")

import C002_people_test_conflict
try:
    C002_people_test_conflict.People_test_conflict()
except Exception as e:
    funcsys.ErrMessage(e)

# CLOSE THE LOG WRITER
funcfile.writelog("Now")
funcfile.writelog("COMPLETED: C002_PEOPLE_TEST_CONFLICT_RUN")
funcfile.writelog("----------------------------------------")

