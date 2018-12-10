
import sys

# Add own module path
sys.path.append('S:/_my_modules')
sys.path.append('S:/Testing/Import')

import funcfile
import funcmail
import funcsys

funcfile.writelog("Now")
funcfile.writelog("SCRIPT: B003_VSS_LISTS_RUN")
funcfile.writelog("--------------------------")

import B003_vss_lists

# Environment
l_mail = True

# Vss lists
try:
    B003_vss_lists.Vss_lists()
except Exception as e:
    funcsys.ErrMessage(e)

# Close the log writer
funcfile.writelog("Now")
funcfile.writelog("COMPLETED: B003_VSS_LISTS_RUN")
funcfile.writelog("----------------------------")
 

