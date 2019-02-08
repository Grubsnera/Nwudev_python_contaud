""" FUNCFILE.PY ****************************************************************
Functions to handle file functions
Copyright (c) AB Janse v Rensburg 24 March 2018
"""

# IMPORT OBJECTS ***************************************************************

# Import the system objects
import datetime
import calendar
import time
import sys

# Import own modules
import funcdate

# FUNCTION TO WRITE INTO LOG FILE **********************************************

def writelog(s_entry="\n",s_path="S:/Logs/",s_file="Python_log_" + datetime.datetime.now().strftime("%Y%m%d") + ".txt"):

    """ Parameter
    s_entry = Text to write
    s_path = Path of the log file (Deafault to
    s_file = Log file name
    """

    try:
        with open(s_path + s_file, 'a') as fl:
            # file opened for writing. write to it here
            # Write the log
            if s_entry == "Now":
                fl.write("----------------\n")
                s_entry = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")+"\n"
            elif "%t" in s_entry:
                s_entry = s_entry.replace("%t",datetime.datetime.now().strftime("%H:%M:%S"))
                s_entry += "\n"
            else:
                s_entry += "\n"
            fl.write(s_entry)
            fl.close()
            pass
    except IOError as x:
        print('error ',x.errno,',', x.strerror)
        """
        if x.errno == errno.EACCES:
            print( s_file, 'no perms')
        elif x.errno == errno.EISDIR:
            print( s_file, 'is directory')
        """

    return
