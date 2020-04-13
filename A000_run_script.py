"""
SCRIPT TO RUN OTHER SCRIPTS
Author: Albert J van Rensburg (NWU:21162395)
Created: 13 Apr 2020
"""

# IMPORT PYTHON PACKAGES

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funcmail
from _my_modules import funcsys

"""
INDEX

GENERAL
UPDATE LOG (A002_log)
VACUUM TEST FINDING TABLES (A003_table_vacuum)
IMPORT TEMP (A001_oracle_to_sqlite(temp))

PEOPLE
IMPORT TEMP (A001_oracle_to_sqlite(people))

VSS
IMPORT TEMP (A001_oracle_to_sqlite(vss))

KFS
IMPORT TEMP (A001_oracle_to_sqlite(kfs))

"""


def run_scripts(s_script: str = "a003", s_parameter1: str = "", s_parameter2: str = ""):
    """
    SCRIPT TO RUN OTHER SCRIPTS
    IT WILL USE GLOBAL l_run VARIABLES
    :param s_script: Script to run
    :param s_parameter1: Script first parameter
    :param s_parameter2: Script second parameter
    :return: bool: Successful or not
    """

    # VARIABLES
    l_return = True
    l_debug = True

    # DEBUG
    if l_debug:
        print("SCRIPT: " + s_script)
        print("PARAM1: " + s_parameter1)
        print("PARAM2: " + s_parameter2)

    # GENERAL GROUP ****************************************************************

    # UPDATE LOG
    if s_script in "a002|all|general":
        import A002_log
        from _my_modules import funcdate
        s_project: str = "A002_log"
        try:
            if s_script == 'a002' and s_parameter1[0:1] == 'y':
                A002_log.log_capture(funcdate.yesterday(), True)
            elif s_script == 'all':
                A002_log.log_capture(funcdate.yesterday(), True)
            else:
                A002_log.log_capture(funcdate.today(), False)
            # SUCCESSFUL EXECUTION
            if funcconf.l_mail_project:
                funcmail.Mail('std_success_gmail',
                              'NWUIACA:Success:' + s_project,
                              'NWUIACA: Success: ' + s_project)
        except Exception as err:
            # UNSUCCESSFUL EXECUTION
            l_return = False
            funcsys.ErrMessage(err, funcconf.l_mail_project,
                               'NWUIACA:Fail:' + s_project,
                               'NWUIACA: Fail: ' + s_project)

    # VACUUM TEST FINDING TABLES
    if s_script in "a003|all|general":
        import A003_table_vacuum
        s_project: str = "A003_table_vacuum"
        try:
            A003_table_vacuum.table_vacuum()
            # SUCCESSFUL EXECUTION
            if funcconf.l_mail_project:
                funcmail.Mail('std_success_gmail',
                              'NWUIACA:Success:' + s_project,
                              'NWUIACA: Success: ' + s_project)
        except Exception as err:
            # UNSUCCESSFUL EXECUTION
            l_return = False
            funcsys.ErrMessage(err, funcconf.l_mail_project,
                               'NWUIACA:Fail:' + s_project,
                               'NWUIACA: Fail: ' + s_project)

    # IMPORT TEMP
    if s_script in "a001":
        import A001_oracle_to_sqlite
        s_project: str = "A001_oracle_to_sqlite(temp)"
        try:
            if s_script == "a001" and s_parameter1[0:1] == "t":
                A001_oracle_to_sqlite.oracle_to_sqlite()
                # SUCCESSFUL EXECUTION
                if funcconf.l_mail_project:
                    funcmail.Mail('std_success_gmail',
                                  'NWUIACA:Success:' + s_project,
                                  'NWUIACA: Success: ' + s_project)
        except Exception as err:
            # UNSUCCESSFUL EXECUTION
            l_return = False
            # DISABLE PEOPLE TESTS
            funcconf.l_run_people_test = False
            # ERROR MESSAGE
            funcsys.ErrMessage(err, funcconf.l_mail_project,
                               "NWUIACA:Fail:" + s_project,
                               "NWUIACA: Fail: " + s_project)

    # PEOPLE GROUP *****************************************************************

    # IMPORT PEOPLE
    if s_script in "a001|all|data|people":
        import A001_oracle_to_sqlite
        s_project: str = "A001_oracle_to_sqlite(people)"
        try:
            if (s_script == "a001" and s_parameter1[0:1] == "p") or (s_script in "all|data|people"):
                A001_oracle_to_sqlite.oracle_to_sqlite("000b_Table - people.csv", "PEOPLE")
                # SUCCESSFUL EXECUTION
                if funcconf.l_mail_project:
                    funcmail.Mail('std_success_gmail',
                                  'NWUIACA:Success:' + s_project,
                                  'NWUIACA: Success: ' + s_project)
        except Exception as err:
            # UNSUCCESSFUL EXECUTION
            l_return = False
            # DISABLE PEOPLE TESTS
            funcconf.l_run_people_test = False
            # ERROR MESSAGE
            funcsys.ErrMessage(err, funcconf.l_mail_project,
                               "NWUIACA:Fail:" + s_project,
                               "NWUIACA: Fail: " + s_project)

    # VSS GROUP ********************************************************************

    # IMPORT VSS
    if s_script in "a001|all|data|people":
        import A001_oracle_to_sqlite
        s_project: str = "A001_oracle_to_sqlite(vss)"
        try:
            if (s_script == "a001" and s_parameter1[0:1] == "v") or (s_script in "all|data|vss"):
                A001_oracle_to_sqlite.oracle_to_sqlite("000b_Table - vss.csv", "VSS")
                # SUCCESSFUL EXECUTION
                if funcconf.l_mail_project:
                    funcmail.Mail('std_success_gmail',
                                  'NWUIACA:Success:' + s_project,
                                  'NWUIACA: Success: ' + s_project)
        except Exception as err:
            # UNSUCCESSFUL EXECUTION
            l_return = False
            # DISABLE VSS TESTS
            funcconf.l_run_vss_test = False
            # ERROR MESSAGE
            funcsys.ErrMessage(err, funcconf.l_mail_project,
                               "NWUIACA:Fail:" + s_project,
                               "NWUIACA: Fail: " + s_project)

    # KFS GROUP ********************************************************************

    # IMPORT KFS
    if s_script in "a001|all|data|kfs":
        import A001_oracle_to_sqlite
        s_project: str = "A001_oracle_to_sqlite(kfs)"
        try:
            if (s_script == "a001" and s_parameter1[0:1] == "k") or (s_script in "all|data|kfs"):
                A001_oracle_to_sqlite.oracle_to_sqlite("000b_Table - kfs.csv", "KFS")
                # SUCCESSFUL EXECUTION
                if funcconf.l_mail_project:
                    funcmail.Mail('std_success_gmail',
                                  'NWUIACA:Success:' + s_project,
                                  'NWUIACA: Success: ' + s_project)
        except Exception as err:
            # UNSUCCESSFUL EXECUTION
            l_return = False
            # DISABLE KFS TESTS
            funcconf.l_run_kfs_test = False
            # ERROR MESSAGE
            funcsys.ErrMessage(err, funcconf.l_mail_project,
                               "NWUIACA:Fail:" + s_project,
                               "NWUIACA: Fail: " + s_project)

    return l_return


if __name__ == '__main__':
    try:
        run_scripts()
    except Exception as e:
        funcsys.ErrMessage(e)


