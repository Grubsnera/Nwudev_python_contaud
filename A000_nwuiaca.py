"""
NWU INTERNAL AUDIT ROBOT USING TELEGRAM MESSAGING
Press Ctrl-C on the command line or send a signal to the process to stop the bot.
Author: Albert B Janse van Rensburg (NWU:21162395)
Date created: 17 Mar 2020
Updated:
"""

# IMPORT PYTHON PACKAGES
import datetime
import logging
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters
from threading import Thread
import time

import A004_import_ia
import B009_ia_lists
# IMPORT OWN MODULES
from _my_modules import funcbott
from _my_modules import funcconf
from _my_modules import funcdate
from _my_modules import funcfile
from _my_modules import funcmail
from _my_modules import funcsms
from _my_modules import funcsys

# SET TO TRUE FOR ACTIVE NWU USE OR COMMENT OUT
funcconf.l_tel_use_nwu = True
s_path: str = "S:/Logs/"

"""
INDEX

START BOT AND CREATE UPDATER (main)

THREAD TO RUN VACUUM SCRIPT (RunVacuum)
VACUUM TEST FINDING TABLES (A003_table_vacuum)(24/7)

THREAD TO RUN LARGE SCRIPT (RunLarge)
BACKUP MYSQL (B008_mysql_backup(web->server))(MonTueWedThuFri)
IMPORT INTERNAL AUDIT (A004_import_ia)(MonTueWedThuFri)
INTERNAL AUDIT LISTS (B009_ia_lists)(MonTueWedThuFri)
IMPORT PEOPLE (A001_oracle_to_sqlite(people))(MonTueWedThuFri)
PEOPLE LISTS (B001_people_lists)(MonTueWedThuFri)
PEOPLE LIST MASTER FILE (C003_people_list_masterfile)(MonTueWedThuFri)
PEOPLE PAYROLL LISTS (B004_payroll_lists)(MonTueWedThuFri)
IMPORT VSS (A001_oracle_to_sqlite(vss))(MonTueWedThuFri)
VSS LISTS (B003_vss_lists)(MonTueWedThuFri)
VSS PERIOD LIST (B007_vss_period_list)(MonTueWedThuFri)
VSS STUDENT DEFERMENT MASTER FILE (C301_report_student_deferment)(MonTueWedThuFri)

THREAD TO RUN SMALL SCRIPT (RunSmall)
IMPORT KFS (A001_oracle_to_sqlite(kfs))(TueWedThuFriSat)
KFS LISTS (B002_kfs_lists)(TueWedThuFriSat)
KFS PERIOD LISTS CURR (B006_kfs_period_list)(TueWedThuFriSat)
KFS PERIOD LISTS PREV (B006_kfs_period_list)(TueWedThuFriSat)
MYSQL UPDATE WEB IA NWU (B005_mysql_lists)(TueWedThuFriSat)

THREAD TO RUN TEST SCRIPT (RunTest)
UPDATE LOG (A002_log) "MonTueWedThuFriSatSun"
PEOPLE TEST MASTER FILE (C001_people_test_masterfile)(MonTueWedThuFri)
PEOPLE TEST CONFLICT (C002_people_test_conflict)(MonTueWedThuFri)
VSS STUDENT DEBTOR RECON (C200_report_studdeb_recon)(MonTueWedThuFri)
VSS STUDENT MASTER FILE TESTS (C300_test_student_general)(Days:01,13)
VSS STUDENT FEE TESTS AND REPORTS (C302_test_student_fee)(MonTueWedThuFri)
KFS CREDITOR PAYMENT TESTS (C201_creditor_test_payments)(MonTueWedThuFri)
KFS GL TRANSACTION TESTS (C202_gl_test_transactions)(MonTueWedThuFri)
"""

# ENABLE LOGGING
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """
    START BOT AND CREATE UPDATER
    :return: Nothing
    """

    # LOGGING
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: OPEN PROJECT NWU INTERNAL AUDIT CONTINUOUS AUDIT")
    funcfile.writelog("--------------------------------------------------------")

    # START THE BOT
    if funcconf.l_tel_use_nwu:
        updater = Updater(funcconf.s_tel_token_nwu, use_context=True)  # NWU
    else:
        updater = Updater(funcconf.s_tel_token_alb, use_context=True)  # Albert

    # GET THE DISPATCHER TO REGISTER HANDLERS
    dp = updater.dispatcher

    # ON DIFFERENT COMMANDS - ANSWER IN TELEGRAM
    dp.add_handler(CommandHandler("name", funcbott.name, pass_args=True))
    dp.add_handler(CommandHandler("hi", funcbott.hi))
    dp.add_handler(CommandHandler("helping", funcbott.helping))
    dp.add_handler(CommandHandler("report", funcbott.report, pass_args=True))
    dp.add_handler(CommandHandler("run", funcbott.run, pass_args=True))
    dp.add_handler(CommandHandler("set", funcbott.set_schedule, pass_args=True))
    dp.add_handler(CommandHandler("switch", funcbott.switch, pass_args=True))

    # ON NON COMMAND - DO NOT UNDERSTAND MESSAGE
    dp.add_handler(CommandHandler("stop", funcbott.stop))
    dp.add_handler(MessageHandler(Filters.text, funcbott.echo))

    # LOG ALL ERRORS
    dp.add_error_handler(funcbott.error)

    # SEND OPENING MESSAGES
    funcsms.send_telegram("Dear", "administrator", "the <b>server</b> is up and running, and you may talk to me!")

    RunVacuum().start()
    RunLarge().start()
    RunSmall().start()
    RunTest().start()

    # START THE BOT
    updater.start_polling()
    # Run the bot until you press Ctrl-C or the process receives SIGINT,
    # SIGTERM or SIGABRT. This should be used most of the time, since
    # start_polling() is non-blocking and will stop the bot gracefully.
    updater.idle()


class RunVacuum(Thread):

    def run(self):
        """
        THREAD TO RUN VACUUM SCRIPT
        :return: Nothing
        """

        # IMPORT OWN MODULES
        # import B008_mysql_backup
        import A003_table_vacuum
        import A004_import_ia
        import B009_ia_lists
        import A001_oracle_to_sqlite
        import B001_people_lists
        import C003_people_list_masterfile
        import B004_payroll_lists
        import B003_vss_lists
        import B007_vss_period_list

        # DECLARE VARIABLES
        l_run_vacuum: bool = True  # Run vacuum thread
        l_clock: bool = False  # Display the local clock
        i_sleep: int = 60  # Sleeping time in seconds

        # SEND MESSAGE TO INDICATE START OF VACUUM THREAD
        if funcconf.l_mess_project:
            funcsms.send_telegram("", "administrator", "Vacuum thread started!")

        # DO UNTIL GLOBAL l_run_system IS FALSE
        while l_run_vacuum:

            if funcconf.l_run_system:

                # DISPLAY THE LOCAL TIME
                if l_clock:
                    print("Vacuum thread" + time.strftime("%T", time.localtime()))

                if time.strftime("%R", time.localtime()) == "00:01":
                    funcfile.writelog("Now", "", "", "w+")
                    funcfile.writelog("SCRIPT: OPEN PROJECT NWU INTERNAL AUDIT CONTINUOUS AUDIT")
                    funcfile.writelog("--------------------------------------------------------")

                # SEND MESSAGE TO INDICATE START OF WORKING DAY
                if funcconf.l_mess_project:
                    if time.strftime("%R", time.localtime()) == "07:45":
                        funcsms.send_telegram("Dear", "administrator",
                                              "your working day started, and I'm up and running!")
                        time.sleep(60)

                # SEND MESSAGE TO INDICATE LUNCH TIME
                if funcconf.l_mess_project:
                    if time.strftime("%R", time.localtime()) == "12:55":
                        funcsms.send_telegram("Dear", "administrator",
                                              "how about going for a walk, while I'm keeping up!")
                        # VACUUM TEST FINDING TABLES
                        try:
                            A003_table_vacuum.table_vacuum()
                            # SEND MAIL AFTER SUCCESSFUL VACUUMING
                            if funcconf.l_mail_project:
                                funcmail.Mail('std_success_gmail',
                                              'NWUIACA:Success:A003_table_vacuum',
                                              'NWUIACA: Success: A003_table_vacuum')
                        except Exception as err:
                            # UNSUCCESSFUL VACUUMING
                            funcsys.ErrMessage(err, funcconf.l_mail_project,
                                               "NWUIACA:Fail:A003_table_vacuum",
                                               "NWUIACA: Fail: A003_table_vacuum")
                        time.sleep(60)

                # SEND MESSAGE TO INDICATE WORKING DAY END
                if funcconf.l_mess_project:
                    if time.strftime("%R", time.localtime()) == "16:30":
                        funcsms.send_telegram("Dear", "administrator",
                                              "you've done your part today, while I'm prepping!")
                        time.sleep(60)

                # RUN THE VACUUM SCRIPT
                if datetime.datetime.now() >= funcconf.d_run_vacuum:

                    """*************************************************************
                    VACUUM SCHEDULE START
                    *************************************************************"""
                    funcfile.writelog("%t THREAD: VACUUM SCHEDULE START")

                    # MESSAGE TO ADMIN
                    if funcconf.l_mess_project:
                        funcsms.send_telegram('', 'administrator', 'Vacuum schedule start.')

                    # SET DATE AND TIME FOR NEXT RUN
                    if time.strftime("%R", time.localtime()) <= "15:55":
                        funcconf.d_run_vacuum = datetime.datetime.strptime(funcdate.today() +
                                                                           " 17:00:00",
                                                                           "%Y-%m-%d %H:%M:%S")
                    else:
                        funcconf.d_run_vacuum = datetime.datetime.strptime(funcdate.today() +
                                                                           " 17:00:00",
                                                                           "%Y-%m-%d %H:%M:%S") \
                                                + datetime.timedelta(days=1)

                    # VACUUM TEST FINDING TABLES
                    try:

                        A003_table_vacuum.table_vacuum()
                        # ENABLE TESTS AFTER SUCCESSFUL VACUUMING
                        funcconf.l_run_kfs_test = True
                        funcconf.l_run_people_test = True
                        funcconf.l_run_vss_test = True
                        # SEND MAIL AFTER SUCCESSFUL VACUUMING
                        if funcconf.l_mail_project:
                            funcmail.Mail('std_success_gmail',
                                          'NWUIACA:Success:A003_table_vacuum',
                                          'NWUIACA: Success: A003_table_vacuum')

                    except Exception as err:

                        # UNSUCCESSFUL VACUUMING
                        funcsys.ErrMessage(err, funcconf.l_mail_project,
                                           "NWUIACA:Fail:A003_table_vacuum",
                                           "NWUIACA: Fail: A003_table_vacuum")

                    # BACKUP MYSQL ********************************************
                    # Cancelled backup as from 26 Oct 2022.
                    """
                    s_project: str = "B008_mysql_backup(web->server)"
                    if funcdate.today_dayname() in "MonTueWedThuFri":
                        try:
                            B008_mysql_backup.mysql_backup()
                            if funcconf.l_mail_project:
                                funcmail.Mail('std_success_gmail',
                                              'NWUIACA:Success:' + s_project,
                                              'NWUIACA: Success: ' + s_project)
                        except Exception as err:
                            # ERROR MESSAGE
                            funcsys.ErrMessage(err, funcconf.l_mail_project,
                                               "NWUIACA:Fail:" + s_project,
                                               "NWUIACA: Fail: " + s_project)
                    else:
                        print("BACKUP " + s_project + " do not run on Saturdays and Sundays")
                        if funcconf.l_mess_project:
                            funcsms.send_telegram("", "administrator", s_project + " do not run sat sun.")
                        funcfile.writelog(
                            "%t SCRIPT: " + s_project.upper() + ": DO NOT RUN ON SATURDAYS AND SUNDAYS")
                    """

                    # IMPORT INTERNAL AUDIT ***********************************
                    s_project: str = "A004_import_ia"
                    if funcconf.l_run_people_test:
                        if funcdate.today_dayname() in "MonTueWedThuFri":
                            try:
                                A004_import_ia.ia_mysql_import()
                                if funcconf.l_mail_project:
                                    funcmail.Mail('std_success_gmail',
                                                  'NWUIACA:Success:' + s_project,
                                                  'NWUIACA: Success: ' + s_project)
                            except Exception as err:
                                # DISABLE VSS TESTS
                                funcconf.l_run_vss_test = False
                                # ERROR MESSAGE
                                funcsys.ErrMessage(err, funcconf.l_mail_project,
                                                   "NWUIACA:Fail:" + s_project,
                                                   "NWUIACA: Fail: " + s_project)
                        else:
                            print("IMPORT FROM WEB to SQLITE " + s_project + " do not run on Saturdays and Sundays")
                            if funcconf.l_mess_project:
                                funcsms.send_telegram("", "administrator", s_project + " do not run sat sun.")
                            funcfile.writelog(
                                "%t SCRIPT: " + s_project.upper() + ": DO NOT RUN ON SATURDAYS AND SUNDAYS")

                    # INTERNAL AUDIT LISTS ************************************
                    # CURRENT YEAR
                    s_project: str = "B009_ia_lists"
                    if funcconf.l_run_people_test:
                        if funcdate.today_dayname() in "MonTueWedThuFri":
                            try:
                                B009_ia_lists.ia_lists("curr")
                                if funcconf.l_mail_project:
                                    funcmail.Mail('std_success_gmail',
                                                  'NWUIACA:Success:' + s_project,
                                                  'NWUIACA: Success: ' + s_project)
                            except Exception as err:
                                # DISABLE VSS TESTS
                                funcconf.l_run_vss_test = False
                                # ERROR MESSAGE
                                funcsys.ErrMessage(err, funcconf.l_mail_project,
                                                   "NWUIACA:Fail:" + s_project,
                                                   "NWUIACA: Fail: " + s_project)
                        else:
                            print("INTERNAL AUDIT LISTS " + s_project + " do not run on Saturdays and Sundays")
                            if funcconf.l_mess_project:
                                funcsms.send_telegram("", "administrator", s_project + " do not run sat sun.")
                            funcfile.writelog(
                                "%t SCRIPT: " + s_project.upper() + ": DO NOT RUN ON SATURDAYS AND SUNDAYS")

                    # PREVIOUS YEAR
                    s_project: str = "B009_ia_lists"
                    if funcconf.l_run_people_test:
                        if funcdate.today_dayname() in "MonTueWedThuFri":
                            try:
                                B009_ia_lists.ia_lists("prev")
                                if funcconf.l_mail_project:
                                    funcmail.Mail('std_success_gmail',
                                                  'NWUIACA:Success:' + s_project,
                                                  'NWUIACA: Success: ' + s_project)
                            except Exception as err:
                                # DISABLE VSS TESTS
                                funcconf.l_run_vss_test = False
                                # ERROR MESSAGE
                                funcsys.ErrMessage(err, funcconf.l_mail_project,
                                                   "NWUIACA:Fail:" + s_project,
                                                   "NWUIACA: Fail: " + s_project)
                        else:
                            print("INTERNAL AUDIT LISTS " + s_project + " do not run on Saturdays and Sundays")
                            if funcconf.l_mess_project:
                                funcsms.send_telegram("", "administrator", s_project + " do not run sat sun.")
                            funcfile.writelog(
                                "%t SCRIPT: " + s_project.upper() + ": DO NOT RUN ON SATURDAYS AND SUNDAYS")

                    # IMPORT PEOPLE ************************************************
                    s_project: str = "A001_oracle_to_sqlite(people)"
                    if funcconf.l_run_people_test:
                        if funcdate.today_dayname() in "MonTueWedThuFri":
                            try:
                                A001_oracle_to_sqlite.oracle_to_sqlite("000b_Table - people.csv", "PEOPLE")
                                if funcconf.l_mail_project:
                                    funcmail.Mail('std_success_gmail',
                                                  'NWUIACA:Success:' + s_project,
                                                  'NWUIACA: Success: ' + s_project)
                            except Exception as err:
                                # DISABLE PEOPLE TESTS
                                funcconf.l_run_people_test = False
                                # ERROR MESSAGE
                                funcsys.ErrMessage(err, funcconf.l_mail_project,
                                                   "NWUIACA:Fail:" + s_project,
                                                   "NWUIACA: Fail: " + s_project)
                        else:
                            print("ORACLE to SQLITE " + s_project + " do not run on Saturdays and Sundays")
                            if funcconf.l_mess_project:
                                funcsms.send_telegram("", "administrator", s_project + " do not run sat sun.")
                            funcfile.writelog(
                                "%t SCRIPT: " + s_project.upper() + ": DO NOT RUN ON SATURDAYS AND SUNDAYS")

                    # IMPORT VSS ***************************************************
                    s_project: str = "A001_oracle_to_sqlite(vss)"
                    if funcconf.l_run_vss_test:
                        if funcdate.today_dayname() in "MonTueWedThuFri":
                            try:
                                A001_oracle_to_sqlite.oracle_to_sqlite("000b_Table - vss.csv", "VSS")
                                if funcconf.l_mail_project:
                                    funcmail.Mail('std_success_gmail',
                                                  'NWUIACA:Success:' + s_project,
                                                  'NWUIACA: Success: ' + s_project)
                            except Exception as err:
                                # DISABLE VSS TESTS
                                funcconf.l_run_vss_test = False
                                # ERROR MESSAGE
                                funcsys.ErrMessage(err, funcconf.l_mail_project,
                                                   "NWUIACA:Fail:" + s_project,
                                                   "NWUIACA: Fail: " + s_project)
                        else:
                            print("ORACLE to SQLITE " + s_project + " do not run on Saturdays and Sundays")
                            if funcconf.l_mess_project:
                                funcsms.send_telegram("", "administrator", s_project + " do not run sat sun.")
                            funcfile.writelog(
                                "%t SCRIPT: " + s_project.upper() + ": DO NOT RUN ON SATURDAYS AND SUNDAYS")

                    # PEOPLE LISTS *************************************************
                    s_project: str = "B001_people_lists"
                    if funcconf.l_run_people_test:
                        if funcdate.today_dayname() in "MonTueWedThuFri":
                            try:
                                B001_people_lists.people_lists()
                                if funcconf.l_mail_project:
                                    funcmail.Mail('std_success_gmail',
                                                  'NWUIACA:Success:' + s_project,
                                                  'NWUIACA: Success: ' + s_project)
                            except Exception as err:
                                # DISABLE PEOPLE TESTS
                                funcconf.l_run_people_test = False
                                # ERROR MESSAGE
                                funcsys.ErrMessage(err, funcconf.l_mail_project,
                                                   "NWUIACA:Fail:" + s_project,
                                                   "NWUIACA: Fail: " + s_project)
                        else:
                            print("ORACLE to SQLITE " + s_project + " do not run on Saturdays and Sundays")
                            if funcconf.l_mess_project:
                                funcsms.send_telegram("", "administrator", s_project + " do not run sat sun.")
                            funcfile.writelog(
                                "%t SCRIPT: " + s_project.upper() + ": DO NOT RUN ON SATURDAYS AND SUNDAYS")

                    # PEOPLE LIST MASTER FILE **************************************
                    s_project: str = "C003_people_list_masterfile"
                    if funcconf.l_run_people_test:
                        if funcdate.today_dayname() in "MonTueWedThuFri":
                            try:
                                C003_people_list_masterfile.people_list_masterfile()
                                if funcconf.l_mail_project:
                                    funcmail.Mail('std_success_gmail',
                                                  'NWUIACA:Success:' + s_project,
                                                  'NWUIACA: Success: ' + s_project)
                            except Exception as err:
                                # DISABLE PEOPLE TESTS
                                # funcconf.l_run_people_test = False
                                # ERROR MESSAGE
                                funcsys.ErrMessage(err, funcconf.l_mail_project,
                                                   "NWUIACA:Fail:" + s_project,
                                                   "NWUIACA: Fail: " + s_project)
                        else:
                            print("ORACLE to SQLITE " + s_project + " do not run on Saturdays and Sundays")
                            if funcconf.l_mess_project:
                                funcsms.send_telegram("", "administrator", s_project + " do not run sat sun.")
                            funcfile.writelog(
                                "%t SCRIPT: " + s_project.upper() + ": DO NOT RUN ON SATURDAYS AND SUNDAYS")

                    # PEOPLE PAYROLL LISTS *****************************************
                    s_project: str = "B004_payroll_lists"
                    if funcconf.l_run_people_test:
                        if funcdate.today_dayname() in "MonTueWedThuFri":
                            try:
                                B004_payroll_lists.payroll_lists()
                                if funcconf.l_mail_project:
                                    funcmail.Mail('std_success_gmail',
                                                  'NWUIACA:Success:' + s_project,
                                                  'NWUIACA: Success: ' + s_project)
                            except Exception as err:
                                # DISABLE PEOPLE TESTS
                                # funcconf.l_run_people_test = False
                                # ERROR MESSAGE
                                funcsys.ErrMessage(err, funcconf.l_mail_project,
                                                   "NWUIACA:Fail:" + s_project,
                                                   "NWUIACA: Fail: " + s_project)
                        else:
                            print("ORACLE to SQLITE " + s_project + " do not run on Saturdays and Sundays")
                            if funcconf.l_mess_project:
                                funcsms.send_telegram("", "administrator", s_project + " do not run sat sun.")
                            funcfile.writelog(
                                "%t SCRIPT: " + s_project.upper() + ": DO NOT RUN ON SATURDAYS AND SUNDAYS")

                    # VSS LISTS ****************************************************
                    s_project: str = "B003_vss_lists"
                    if funcconf.l_run_vss_test:
                        if funcdate.today_dayname() in "MonTueWedThuFri":
                            try:
                                B003_vss_lists.vss_lists()
                                if funcconf.l_mail_project:
                                    funcmail.Mail('std_success_gmail',
                                                  'NWUIACA:Success:' + s_project,
                                                  'NWUIACA: Success: ' + s_project)
                            except Exception as err:
                                # DISABLE VSS TESTS
                                funcconf.l_run_vss_test = False
                                # ERROR MESSAGE
                                funcsys.ErrMessage(err, funcconf.l_mail_project,
                                                   "NWUIACA:Fail:" + s_project,
                                                   "NWUIACA: Fail: " + s_project)
                        else:
                            print("ORACLE to SQLITE " + s_project + " do not run on Saturdays and Sundays")
                            if funcconf.l_mess_project:
                                funcsms.send_telegram("", "administrator", s_project + " do not run sat sun.")
                            funcfile.writelog(
                                "%t SCRIPT: " + s_project.upper() + ": DO NOT RUN ON SATURDAYS AND SUNDAYS")

                    # VSS PERIOD LIST CURR *****************************************
                    s_project: str = "B007_vss_periof_list(curr)"
                    if funcconf.l_run_vss_test:
                        if funcdate.today_dayname() in "MonTueWedThuFri":
                            try:
                                B007_vss_period_list.vss_period_list("curr")
                                if funcconf.l_mail_project:
                                    funcmail.Mail('std_success_gmail',
                                                  'NWUIACA:Success:' + s_project,
                                                  'NWUIACA: Success: ' + s_project)
                            except Exception as err:
                                # DISABLE VSS TESTS
                                funcconf.l_run_vss_test = False
                                # ERROR MESSAGE
                                funcsys.ErrMessage(err, funcconf.l_mail_project,
                                                   "NWUIACA:Fail:" + s_project,
                                                   "NWUIACA: Fail: " + s_project)
                        else:
                            print("ORACLE to SQLITE " + s_project + " do not run on Saturdays and Sundays")
                            if funcconf.l_mess_project:
                                funcsms.send_telegram("", "administrator", s_project + " do not run sat sun.")
                            funcfile.writelog(
                                "%t SCRIPT: " + s_project.upper() + ": DO NOT RUN ON SATURDAYS AND SUNDAYS")

                    # MESSAGE TO ADMIN
                    if funcconf.l_mess_project:
                        funcsms.send_telegram('', 'administrator', 'Vacuum schedule end.')

                    """*************************************************************
                    VACUUM SCHEDULE END
                    *************************************************************"""
                    funcfile.writelog("%t THREAD: VACUUM SCHEDULE END")

            # STOP PROJECT
            if funcconf.l_stop_project:
                break

            # SLEEPER
            time.sleep(i_sleep)


class RunLarge(Thread):

    def run(self):
        """
        THREAD TO RUN LARGE SCRIPT
        :return: Nothing
        """

        # IMPORT SCRIPTS
        # import B008_mysql_backup
        # import A001_oracle_to_sqlite
        # import B001_people_lists
        # import C003_people_list_masterfile
        # import B004_payroll_lists
        # import B003_vss_lists
        # import B007_vss_period_list
        import C301_report_student_deferment

        # DECLARE VARIABLES
        l_run_large: bool = True  # Run large thread
        l_clock: bool = False  # Display the local clock
        i_sleep: int = 60  # Sleeping time in seconds

        # SEND MESSAGE TO INDICATE START OF LARGE THREAD
        if funcconf.l_mess_project:
            funcsms.send_telegram("", "administrator", "Large thread started!")

        # DO UNTIL GLOBAL l_run_system IS FALSE
        while l_run_large:

            # MESSAGE TO ADMIN
            # if funcconf.l_mess_project:
            #    funcsms.send_telegram('', 'administrator', 'Run large still active.')

            # SLEEPER
            # time.sleep(i_sleep)

            if funcconf.l_run_system:

                # MESSAGE TO ADMIN
                # if funcconf.l_mess_project:
                #     funcsms.send_telegram('', 'administrator', 'Run system still active.')

                # DISPLAY THE LOCAL TIME
                if l_clock:
                    print("LARGE thread time: " + time.strftime("%T", time.localtime()))
                    print(funcconf.d_run_large)

                # RUN THE LARGE SCRIPT ON SCHEDULE
                if datetime.datetime.now() >= funcconf.d_run_large:

                    """*************************************************************
                    LARGE SCHEDULE START
                    *************************************************************"""
                    funcfile.writelog("%t THREAD: LARGE SCHEDULE START")

                    # MESSAGE TO ADMIN
                    if funcconf.l_mess_project:
                        funcsms.send_telegram('', 'administrator', 'Large schedule start.')

                    # SET DATE AND TIME FOR NEXT RUN
                    if time.strftime("%R", time.localtime()) <= "19:55":
                        funcconf.d_run_large = datetime.datetime.strptime(funcdate.today() +
                                                                          ' 21:00:00',
                                                                          "%Y-%m-%d %H:%M:%S")
                    else:
                        funcconf.d_run_large = datetime.datetime.strptime(funcdate.today() +
                                                                          " 21:00:00",
                                                                          "%Y-%m-%d %H:%M:%S") \
                                               + datetime.timedelta(days=1)

                    # VSS STUDENT DEFERMENT MASTER FILE ****************************
                    s_project: str = "C301_report_student_deferment"
                    if funcconf.l_run_vss_test:
                        if funcdate.today_dayname() in "MonTueWedThuFri":
                            try:
                                C301_report_student_deferment.studdeb_deferments()
                                if funcconf.l_mail_project:
                                    funcmail.Mail('std_success_gmail',
                                                  'NWUIACA:Success:' + s_project,
                                                  'NWUIACA: Success: ' + s_project)
                            except Exception as err:
                                # DISABLE VSS TESTS
                                # funcconf.l_run_vss_test = False
                                # ERROR MESSAGE
                                funcsys.ErrMessage(err, funcconf.l_mail_project,
                                                   "NWUIACA:Fail:" + s_project,
                                                   "NWUIACA: Fail: " + s_project)
                        else:
                            print("ORACLE to SQLITE " + s_project + " do not run on Saturdays and Sundays")
                            if funcconf.l_mess_project:
                                funcsms.send_telegram("", "administrator", s_project + " do not run sat sun.")
                            funcfile.writelog(
                                "%t SCRIPT: " + s_project.upper() + ": DO NOT RUN ON SATURDAYS AND SUNDAYS")

                    # MESSAGE TO ADMIN
                    if funcconf.l_mess_project:
                        funcsms.send_telegram('', 'administrator', 'Large schedule end.')

                    """*************************************************************
                    END OF LARGE SCHEDULE
                    *************************************************************"""
                    funcfile.writelog("END OF LARGE SCHEDULE")

                    # SEND MAIL TO INDICATE THE SUCCESSFUL COMPLETION OF LARGE SCHEDULE
                    if funcconf.l_mail_project:
                        funcmail.Mail('std_success_gmail', 'Python:Success:Finished:LargeSchedule',
                                      'NWUIAPython: Success: Finished : Large schedule')
                        funcmail.Mail("python_log")

            # STOP PROJECT
            if funcconf.l_stop_project:
                break

            # SLEEPER
            time.sleep(i_sleep)


class RunSmall(Thread):

    def run(self):
        """
        THREAD TO RUN SMALL SCRIPT
        :return: Nothing
        """

        # IMPORT SCRIPTS
        import A001_oracle_to_sqlite
        import B002_kfs_lists
        import B005_mysql_lists
        import B006_kfs_period_list

        # DECLARE VARIABLES
        l_run_small: bool = True  # Run small thread
        l_clock: bool = False  # Display the local clock
        i_sleep: int = 60  # Sleeping time in seconds

        # SEND MESSAGE TO INDICATE START OF SMALL THREAD
        if funcconf.l_mess_project:
            funcsms.send_telegram("", "administrator", "Small thread started!")

        # DO UNTIL GLOBAL l_run_system IS FALSE
        while l_run_small:

            if funcconf.l_run_system:

                # DISPLAY THE LOCAL TIME
                if l_clock:
                    print("SMALL thread" + time.strftime("%T", time.localtime()))

                # RUN THE SMALL SCHEDULE
                if datetime.datetime.now() >= funcconf.d_run_small:

                    # LOGGING
                    # s_file = "Python_log_" + datetime.datetime.now().strftime("%Y%m%d") + ".txt"
                    # funcfile.writelog("Now", s_path, s_file)
                    # funcfile.writelog("Now","","","w+")
                    # funcfile.writelog("SCRIPT: OPEN PROJECT NWU INTERNAL AUDIT CONTINUOUS AUDIT")
                    # funcfile.writelog("--------------------------------------------------------")

                    """*************************************************************
                    SMALL SCHEDULE START
                    *************************************************************"""
                    funcfile.writelog("%t THREAD: SMALL SCHEDULE START")

                    # MESSAGE TO ADMIN
                    if funcconf.l_mess_project:
                        funcsms.send_telegram('', 'administrator', '<b>Small schedule started</b>')

                    # SET DATE AND TIME FOR NEXT RUN
                    if time.strftime("%R", time.localtime()) <= "23:59":
                        funcconf.d_run_small = datetime.datetime.strptime(funcdate.today() + " 23:02:00",
                                                                          "%Y-%m-%d %H:%M:%S") + \
                                               datetime.timedelta(hours=2)
                    else:
                        funcconf.d_run_small = datetime.datetime.strptime(funcdate.today() + " 23:02:00",
                                                                          "%Y-%m-%d %H:%M:%S") + \
                                               datetime.timedelta(days=1, hours=2)

                    # IMPORT KFS ***************************************************
                    s_project: str = "A001_oracle_to_sqlite(kfs)"
                    if funcconf.l_run_kfs_test:
                        if funcdate.today_dayname() in "TueWedThuFriSat":
                            try:
                                A001_oracle_to_sqlite.oracle_to_sqlite("000b_Table - kfs.csv", "KFS")
                                if funcconf.l_mail_project:
                                    funcmail.Mail('std_success_gmail',
                                                  'NWUIACA:Success:' + s_project,
                                                  'NWUIACA: Success: ' + s_project)
                            except Exception as err:
                                # DISABLE KFS TESTS
                                funcconf.l_run_kfs_test = False
                                funcsys.ErrMessage(err, funcconf.l_mail_project,
                                                   "NWUIACA:Fail:" + s_project,
                                                   "NWUIACA: Fail: " + s_project)
                        else:
                            print("ORACLE to SQLITE " + s_project + " do not run on Sundays and Mondays")
                            if funcconf.l_mess_project:
                                funcsms.send_telegram("", "administrator", s_project + " do not run sun mon.")
                            funcfile.writelog("%t SCRIPT: " + s_project.upper() + ": DO NOT RUN ON SUNDAYS AND MONDAYS")

                    # KFS LISTS ****************************************************
                    s_project: str = "B002_kfs_lists"
                    if funcconf.l_run_kfs_test:
                        if funcdate.today_dayname() in "TueWedThuFriSat":
                            try:
                                B002_kfs_lists.kfs_lists()
                                if funcconf.l_mail_project:
                                    funcmail.Mail('std_success_gmail',
                                                  'NWUIACA:Success:' + s_project,
                                                  'NWUIACA: Success: ' + s_project)
                            except Exception as err:
                                # DISABLE PEOPLE TESTS
                                funcconf.l_run_kfs = False
                                # ERROR MESSAGE
                                funcsys.ErrMessage(err, funcconf.l_mail_project,
                                                   "NWUIACA:Fail:" + s_project,
                                                   "NWUIACA: Fail: " + s_project)
                        else:
                            print("ORACLE to SQLITE " + s_project + " do not run on Sundays Mondays")
                            if funcconf.l_mess_project:
                                funcsms.send_telegram("", "administrator", s_project + " do not run sun mon.")
                            funcfile.writelog("%t SCRIPT: " + s_project.upper() + ": DO NOT RUN ON SUNDAYS MONDAYS")

                    # KFS PERIOD LISTS CURR ****************************************
                    s_project: str = "B006_kfs_period_list(curr)"
                    if funcconf.l_run_kfs_test:
                        if funcdate.today_dayname() in "TueWedThuFriSat":
                            try:
                                B006_kfs_period_list.kfs_period_list("curr")
                                if funcconf.l_mail_project:
                                    funcmail.Mail('std_success_gmail',
                                                  'NWUIACA:Success:' + s_project,
                                                  'NWUIACA: Success: ' + s_project)
                            except Exception as err:
                                # DISABLE PEOPLE TESTS
                                funcconf.l_run_kfs = False
                                # ERROR MESSAGE
                                funcsys.ErrMessage(err, funcconf.l_mail_project,
                                                   "NWUIACA:Fail:" + s_project,
                                                   "NWUIACA: Fail: " + s_project)
                        else:
                            print("ORACLE to SQLITE " + s_project + " do not run on Sundays Mondays")
                            if funcconf.l_mess_project:
                                funcsms.send_telegram("", "administrator", s_project + " do not run sun mon.")
                            funcfile.writelog("%t SCRIPT: " + s_project.upper() + ": DO NOT RUN ON SUNDAYS MONDAYS")

                    # KFS PERIOD LISTS PREV ****************************************
                    # s_project: str = "B006_kfs_period_list(prev)"
                    # if funcconf.l_run_kfs_test:
                    #    if funcdate.today_dayname() in "TueWedThuFriSat":
                    #        try:
                    #            B006_kfs_period_list.kfs_period_list("prev")
                    #            if funcconf.l_mail_project:
                    #                funcmail.Mail('std_success_gmail',
                    #                              'NWUIACA:Success:' + s_project,
                    #                              'NWUIACA: Success: ' + s_project)
                    #        except Exception as err:
                    #            # DISABLE PEOPLE TESTS
                    #            funcconf.l_run_kfs = False
                    #            # ERROR MESSAGE
                    #            funcsys.ErrMessage(err, funcconf.l_mail_project,
                    #                               "NWUIACA:Fail:" + s_project,
                    #                               "NWUIACA: Fail: " + s_project)
                    #    else:
                    #        print("ORACLE to SQLITE " + s_project + " do not run on Sundays Mondays")
                    #        if funcconf.l_mess_project:
                    #            funcsms.send_telegram("", "administrator", s_project + " do not run sun mon.")
                    #        funcfile.writelog("%t SCRIPT: " + s_project.upper() + ": DO NOT RUN ON SUNDAYS MONDAYS")

                    # MYSQL UPDATE WEB IA NWU **************************************
                    s_project: str = "B005_mysql_lists(web)"
                    if funcconf.l_run_people_test:
                        if funcdate.today_dayname() in "TueWedThuFriSat":
                            try:
                                B005_mysql_lists.mysql_lists("Web_ia_nwu")
                                if funcconf.l_mail_project:
                                    funcmail.Mail('std_success_gmail',
                                                  'NWUIACA:Success:' + s_project,
                                                  'NWUIACA: Success: ' + s_project)
                            except Exception as err:
                                # ERROR MESSAGE
                                funcsys.ErrMessage(err, funcconf.l_mail_project,
                                                   "NWUIACA:Fail:" + s_project,
                                                   "NWUIACA: Fail: " + s_project)
                        else:
                            print("ORACLE to SQLITE " + s_project + " do not run on Sundays Mondays")
                            if funcconf.l_mess_project:
                                funcsms.send_telegram("", "administrator", s_project + " do not run sun mon.")
                            funcfile.writelog("%t SCRIPT: " + s_project.upper() + ": DO NOT RUN ON SUNDAYS MONDAYS")

                    # SET THE TEST SCHEDULE TWO MINUTES AFTER COMPLETING
                    funcconf.d_run_test = datetime.datetime.now() + datetime.timedelta(minutes=2)

                    # MESSAGE TO ADMIN
                    if funcconf.l_mess_project:
                        funcsms.send_telegram('', 'administrator', '<b>Small schedule ended</b>')

                    """*************************************************************
                    SMALL SCHEDULE END
                    *************************************************************"""
                    funcfile.writelog("SMALL SCHEDULE END")

                    # SEND MAIL TO INDICATE THE SUCCESSFUL COMPLETION OF SMALL SCHEDULE
                    if funcconf.l_mail_project:
                        funcmail.Mail('std_success_gmail', 'Python:Success:Finished:SmallSchedule',
                                      'NWUIAPython: Success: Finished : Small schedule')

            # STOP PROJECT
            if funcconf.l_stop_project:
                break

            # SLEEPER
            time.sleep(i_sleep)


class RunTest(Thread):

    def run(self):
        """
        THREAD TO RUN TEST SCRIPT
        :return: Nothing
        """

        # IMPORT SCRIPTS
        import A002_log
        import C001_people_test_masterfile
        import C002_people_test_conflict
        import C200_report_studdeb_recon
        import C300_test_student_general
        import C302_test_student_fee
        import C201_creditor_test_payments
        import C202_gl_test_transactions

        # DECLARE VARIABLES
        l_run_test: bool = True  # Run test thread
        l_clock: bool = False  # Display the local clock
        i_sleep: int = 60  # Sleeping time in seconds

        # SEND MESSAGE TO INDICATE START OF TEST THREAD
        if funcconf.l_mess_project:
            funcsms.send_telegram("", "administrator", "Test thread started!")

        # DO UNTIL GLOBAL l_run_system IS FALSE
        while l_run_test:

            if funcconf.l_run_system:

                # DISPLAY THE LOCAL TIME
                if l_clock:
                    print("TEST thread" + time.strftime("%T", time.localtime()))

                # RUN THE TEST SCHEDULE
                if datetime.datetime.now() >= funcconf.d_run_test:

                    """*************************************************************
                    TEST SCHEDULE START
                    *************************************************************"""
                    funcfile.writelog("%t THREAD: TEST SCHEDULE START")

                    # MESSAGE TO ADMIN
                    if funcconf.l_mess_project:
                        funcsms.send_telegram('', 'administrator', '<b>Test schedule started</b>')

                    # SET DATE AND TIME FOR NEXT RUN
                    if time.strftime("%R", time.localtime()) <= "23:59":
                        funcconf.d_run_test = datetime.datetime.strptime(funcdate.today() + " 23:00:00",
                                                                         "%Y-%m-%d %H:%M:%S") + \
                                              datetime.timedelta(hours=7)
                    else:
                        funcconf.d_run_test = datetime.datetime.strptime(funcdate.today() + " 23:00:00",
                                                                         "%Y-%m-%d %H:%M:%S") + \
                                              datetime.timedelta(days=1, hours=7)

                    # UPDATE LOG ***************************************************
                    s_project: str = "A002_log"
                    if funcdate.today_dayname() in "MonTueWedThuFriSatSun":
                        try:
                            A002_log.log_capture(funcdate.yesterday(), True)
                            if funcconf.l_mail_project:
                                funcmail.Mail('std_success_gmail',
                                              'NWUIACA:Success:' + s_project,
                                              'NWUIACA: Success: ' + s_project)
                        except Exception as err:
                            funcsys.ErrMessage(err, funcconf.l_mail_project,
                                               "NWUIACA:Fail:" + s_project,
                                               "NWUIACA: Fail: " + s_project)
                    else:
                        print("ORACLE to SQLITE " + s_project + " do not run on Sundays and Mondays")
                        if funcconf.l_mess_project:
                            funcsms.send_telegram("", "administrator", s_project + " do not run sun mon.")
                        funcfile.writelog("%t SCRIPT: " + s_project.upper() + ": DO NOT RUN ON SUNDAYS AND MONDAYS")

                    # PEOPLE TEST MASTER FILE **************************************
                    s_project: str = "C001_people_test_masterfile"
                    if funcconf.l_run_people_test:
                        if funcdate.today_dayname() in "MonTueWedThuFri":
                            try:
                                C001_people_test_masterfile.people_test_masterfile()
                                if funcconf.l_mail_project:
                                    funcmail.Mail('std_success_gmail',
                                                  'NWUIACA:Success:' + s_project,
                                                  'NWUIACA: Success: ' + s_project)
                            except Exception as err:
                                # DISABLE PEOPLE TESTS
                                # funcconf.l_run_people_test = False
                                # ERROR MESSAGE
                                funcsys.ErrMessage(err, funcconf.l_mail_project,
                                                   "NWUIACA:Fail:" + s_project,
                                                   "NWUIACA: Fail: " + s_project)
                        else:
                            print("ORACLE to SQLITE " + s_project + " do not run on Saturdays and Sundays")
                            if funcconf.l_mess_project:
                                funcsms.send_telegram("", "administrator", s_project + " do not run sat sun.")
                            funcfile.writelog(
                                "%t SCRIPT: " + s_project.upper() + ": DO NOT RUN ON SATURDAYS AND SUNDAYS")

                    # PEOPLE TEST CONFLICT *****************************************
                    s_project: str = "C002_people_test_conflict"
                    if funcconf.l_run_people_test:
                        if funcdate.today_dayname() in "MonTueWedThuFri":
                            try:
                                C002_people_test_conflict.people_test_conflict()
                                if funcconf.l_mail_project:
                                    funcmail.Mail('std_success_gmail',
                                                  'NWUIACA:Success:' + s_project,
                                                  'NWUIACA: Success: ' + s_project)
                            except Exception as err:
                                # DISABLE PEOPLE TESTS
                                # funcconf.l_run_people_test = False
                                # ERROR MESSAGE
                                funcsys.ErrMessage(err, funcconf.l_mail_project,
                                                   "NWUIACA:Fail:" + s_project,
                                                   "NWUIACA: Fail: " + s_project)
                        else:
                            print("ORACLE to SQLITE " + s_project + " do not run on Saturdays and Sundays")
                            if funcconf.l_mess_project:
                                funcsms.send_telegram("", "administrator", s_project + " do not run sat sun.")
                            funcfile.writelog(
                                "%t SCRIPT: " + s_project.upper() + ": DO NOT RUN ON SATURDAYS AND SUNDAYS")

                    # VSS STUDENT DEBTOR RECON *************************************
                    s_project: str = "C200_report_studdeb_recon"
                    if funcconf.l_run_people_test and funcconf.l_run_kfs_test and funcconf.l_run_vss_test:
                        if funcdate.today_dayname() in "MonTueWedThuFri":
                            try:
                                # C200_report_studdeb_recon.report_studdeb_recon()
                                # 2022 balances
                                C200_report_studdeb_recon.report_studdeb_recon(0, 0, 0, "curr")
                                # C200_report_studdeb_recon.report_studdeb_recon(40960505.33, 6573550.30, 29005168.76, "curr")
                                # 2021 balances
                                # C200_report_studdeb_recon.report_studdeb_recon(65676774.13, 61655697.80, 41648563.00, "curr")
                                # 2020 balances
                                # C200_report_studdeb_recon.report_studdeb_recon(48501952.09, -12454680.98, 49976048.39, "curr")
                                # 2019 balances C200_report_studdeb_recon.report_studdeb_recon(66561452.48, -18340951.06, 39482933.18, "prev")
                                if funcconf.l_mail_project:
                                    funcmail.Mail('std_success_gmail',
                                                  'NWUIACA:Success:' + s_project,
                                                  'NWUIACA: Success: ' + s_project)
                            except Exception as err:
                                # DISABLE PEOPLE TESTS
                                # funcconf.l_run_people_test = False
                                # ERROR MESSAGE
                                funcsys.ErrMessage(err, funcconf.l_mail_project,
                                                   "NWUIACA:Fail:" + s_project,
                                                   "NWUIACA: Fail: " + s_project)
                        else:
                            print("ORACLE to SQLITE " + s_project + " do not run on Saturdays and Sundays")
                            if funcconf.l_mess_project:
                                funcsms.send_telegram("", "administrator", s_project + " do not run sat sun.")
                            funcfile.writelog(
                                "%t SCRIPT: " + s_project.upper() + ": DO NOT RUN ON SATURDAYS AND SUNDAYS")

                    # VSS STUDENT MASTER FILE TESTS ********************************
                    s_project: str = "C300_test_student_general"
                    if funcconf.l_run_vss_test:
                        if funcdate.today() in "01z13":
                            try:
                                C300_test_student_general.test_student_general()
                                if funcconf.l_mail_project:
                                    funcmail.Mail('std_success_gmail',
                                                  'NWUIACA:Success:' + s_project,
                                                  'NWUIACA: Success: ' + s_project)
                            except Exception as err:
                                # DISABLE PEOPLE TESTS
                                # funcconf.l_run_people_test = False
                                # ERROR MESSAGE
                                funcsys.ErrMessage(err, funcconf.l_mail_project,
                                                   "NWUIACA:Fail:" + s_project,
                                                   "NWUIACA: Fail: " + s_project)
                        else:
                            print("ORACLE to SQLITE " + s_project + " only run on 1st and 13th of month")
                            if funcconf.l_mess_project:
                                funcsms.send_telegram("", "administrator", s_project + " run 1st and 13th days.")
                            funcfile.writelog(
                                "%t SCRIPT: " + s_project.upper() + ": RUN ON 1ST and 13TH DAYS OF MONTH")

                    # VSS STUDENT FEE TESTS AND REPORTS ****************************
                    s_project: str = "C302_test_student_fee"
                    if funcconf.l_run_vss_test:
                        if funcdate.today_dayname() in "MonTueWedThuFri":
                            try:
                                C302_test_student_fee.student_fee()
                                if funcconf.l_mail_project:
                                    funcmail.Mail('std_success_gmail',
                                                  'NWUIACA:Success:' + s_project,
                                                  'NWUIACA: Success: ' + s_project)
                            except Exception as err:
                                # DISABLE PEOPLE TESTS
                                # funcconf.l_run_people_test = False
                                # ERROR MESSAGE
                                funcsys.ErrMessage(err, funcconf.l_mail_project,
                                                   "NWUIACA:Fail:" + s_project,
                                                   "NWUIACA: Fail: " + s_project)
                        else:
                            print("ORACLE to SQLITE " + s_project + " only run on 1st and 13th of month")
                            if funcconf.l_mess_project:
                                funcsms.send_telegram("", "administrator", s_project + " run 1st and 13th days.")
                            funcfile.writelog(
                                "%t SCRIPT: " + s_project.upper() + ": RUN ON 1ST and 13TH DAYS OF MONTH")

                    # KFS CREDITOR PAYMENT TESTS ***********************************
                    s_project: str = "C201_creditor_test_payments"
                    if funcconf.l_run_kfs_test:
                        if funcdate.today_dayname() in "MonTueWedThuFri":
                            try:
                                C201_creditor_test_payments.creditor_test_payments()
                                if funcconf.l_mail_project:
                                    funcmail.Mail('std_success_gmail',
                                                  'NWUIACA:Success:' + s_project,
                                                  'NWUIACA: Success: ' + s_project)
                            except Exception as err:
                                # DISABLE PEOPLE TESTS
                                # funcconf.l_run_kfs_test = False
                                # ERROR MESSAGE
                                funcsys.ErrMessage(err, funcconf.l_mail_project,
                                                   "NWUIACA:Fail:" + s_project,
                                                   "NWUIACA: Fail: " + s_project)
                        else:
                            print("ORACLE to SQLITE " + s_project + " do not run on Saturdays and Sundays")
                            if funcconf.l_mess_project:
                                funcsms.send_telegram("", "administrator", s_project + " do not run sat sun.")
                            funcfile.writelog(
                                "%t SCRIPT: " + s_project.upper() + ": DO NOT RUN ON SATURDAYS AND SUNDAYS")

                    # KFS GL TRANSACTION TESTS *************************************
                    s_project: str = "C202_gl_test_transactions"
                    if funcconf.l_run_kfs_test:
                        if funcdate.today_dayname() in "MonTueWedThuFri":
                            try:
                                C202_gl_test_transactions.gl_test_transactions()
                                if funcconf.l_mail_project:
                                    funcmail.Mail('std_success_gmail',
                                                  'NWUIACA:Success:' + s_project,
                                                  'NWUIACA: Success: ' + s_project)
                            except Exception as err:
                                # DISABLE PEOPLE TESTS
                                # funcconf.l_run_kfs_test = False
                                # ERROR MESSAGE
                                funcsys.ErrMessage(err, funcconf.l_mail_project,
                                                   "NWUIACA:Fail:" + s_project,
                                                   "NWUIACA: Fail: " + s_project)
                        else:
                            print("ORACLE to SQLITE " + s_project + " do not run on Saturdays and Sundays")
                            if funcconf.l_mess_project:
                                funcsms.send_telegram("", "administrator", s_project + " do not run sat sun.")
                            funcfile.writelog(
                                "%t SCRIPT: " + s_project.upper() + ": DO NOT RUN ON SATURDAYS AND SUNDAYS")

                    # MESSAGE TO ADMIN *********************************************
                    if funcconf.l_mess_project:
                        funcsms.send_telegram('', 'administrator', '<b>Test schedule ended</b>')

                    """*************************************************************
                    TEST SCHEDULE END
                    *************************************************************"""
                    funcfile.writelog("TEST SCHEDULE END")

                    # SEND MAIL TO INDICATE THE SUCCESSFUL COMPLETION OF TEST SCHEDULE
                    if funcconf.l_mail_project:
                        funcmail.Mail('std_success_gmail', 'Python:Success:Finished:TestSchedule',
                                      'NWUIAPython: Success: Finished : Test schedule')
                        funcmail.Mail("python_log")

            # STOP PROJECT
            if funcconf.l_stop_project:
                break

            # SLEEPER
            time.sleep(i_sleep)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        funcsys.ErrMessage(e)
