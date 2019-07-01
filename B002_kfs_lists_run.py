"""
Script to EXECUTE the KFS LISTS script.
"""

# IMPORT OWN MODULES
from _my_modules import funcfile
from _my_modules import funcsys
import B002_kfs_lists

# OPEN THE LOG WRITER
funcfile.writelog("Now")
funcfile.writelog("SCRIPT: A000_RUN_ALL")
funcfile.writelog("--------------------")

# RUN KFS LISTS
try:
    B002_kfs_lists.kfs_lists()
except Exception as e:
    funcsys.ErrMessage(e)

# CLOSE THE LOG WRITER
funcfile.writelog("Now")
funcfile.writelog("COMPLETED: A000_RUN_ALL")
funcfile.writelog("-----------------------")
