"""
Functions to handle file functions
Author: AB Janse v Rensburg
Create: 24 Jan 2018
"""

# IMPORT SYSTEM OBJECTS
import datetime


def writelog(s_entry="\n", s_path="S:/Logs/",
             s_file="Python_log_" + datetime.datetime.now().strftime("%Y%m%d") + ".txt"):
    """
    Function to create log file
    :param s_entry: Log file entry
    :param s_path: Log file path
    :param s_file: Log file name
    :return: Nothing
    """

    try:
        with open(s_path + s_file, 'a') as fl:
            # file opened for writing. write to it here
            # Write the log
            if s_entry == "Now":
                fl.write("----------------\n")
                s_entry = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")+"\n"
            elif "%t" in s_entry:
                s_entry = s_entry.replace("%t", datetime.datetime.now().strftime("%H:%M:%S"))
                s_entry += "\n"
            else:
                s_entry += "\n"
            fl.write(s_entry)
            fl.close()
            pass
    except IOError as x:
        print('error ', x.errno, ',', x.strerror)

    return
