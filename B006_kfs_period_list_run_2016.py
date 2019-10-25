"""
Script to EXECUTE the KFS LISTS script.
"""

# IMPORT OWN MODULES
from _my_modules import funcfile
from _my_modules import funcsys
import B006_kfs_period_list

# OPEN THE LOG WRITER
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: B006_RUN_KFS_PERIOD_LIST")
funcfile.writelog("--------------------------------")

# RUN KFS LISTS
try:
    B006_kfs_period_list.kfs_period_list("2016","2016")
except Exception as e:
    funcsys.ErrMessage(e)

# CLOSE THE LOG WRITER
funcfile.writelog("Now")
funcfile.writelog("COMPLETED: B006_KFS_PERIOD_LIST")
funcfile.writelog("-------------------------------")
