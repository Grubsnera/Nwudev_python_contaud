"""
NWU Internal Audit Bot to handle Telegram messages
Press Ctrl-C on the command line or send a signal to the process to stop the bot.
Author: Albert B Janse van Rensburg (NWU:21162395)
Date created: 17 Mar 2020
Updated:
"""

# IMPORT PYTHON PACKAGES
import logging
from threading import Thread

# IMPORT OWN MODULES
from _my_modules import funcconf

# SET TO TRUE FOR ACTIVE NWU USE OR COMMENT OUT
funcconf.l_tel_use_nwu = True

# ENABLE LOGGING
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """
    Create the updater
    :return: Nothing
    """

    # IMPORT PYTHON PACKAGES
    from telegram.ext import Updater, CommandHandler, MessageHandler, Filters

    # IMPORT OWN MODULES
    from _my_modules import funcbott
    from _my_modules import funcfile
    from _my_modules import funcsms

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
    dp.add_handler(CommandHandler("hi", funcbott.hi))
    dp.add_handler(CommandHandler("help", funcbott.help))
    dp.add_handler(CommandHandler("set", funcbott.set_schedule, pass_args=True))

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
        Thread to execute the vacuum script.
        :return:
        """

        # IMPORT PYTHON PACKAGES
        import datetime
        import time

        # IMPORT OWN MODULES
        from _my_modules import funcdate
        from _my_modules import funcmail
        from _my_modules import funcsms
        from _my_modules import funcsys
        import A003_table_vacuum

        # DECLARE VARIABLES
        l_clock: bool = False  # Display the local clock
        i_sleep: int = 60  # Sleeping time in seconds

        # SEND MESSAGE TO INDICATE START OF VACUUM THREAD
        if funcconf.l_mess_project:
            funcsms.send_telegram("Dear", "administrator", "vacuum thread started!")

        # DO UNTIL GLOBAL l_run_project IS FALSE
        while funcconf.l_run_project:

            # DISPLAY THE LOCAL TIME
            if l_clock:
                print("VACUUM thread" + time.strftime("%T", time.localtime()))

            # SEND MESSAGE TO INDICATE START OF WORKING DAY
            if funcconf.l_mess_project:
                if time.strftime("%R", time.localtime()) == "07:45":
                    funcsms.send_telegram("Dear", "administrator", "your working day started, and I'm up and running!")
                    time.sleep(60)

            # SEND MESSAGE TO INDICATE LUNCH TIME
            if funcconf.l_mess_project:
                if time.strftime("%R", time.localtime()) == "12:55":
                    funcsms.send_telegram("Dear", "administrator", "how about going for a walk, while I'm keeping up!")
                    time.sleep(60)

            # SEND MESSAGE TO INDICATE WORKING DAY END
            if funcconf.l_mess_project:
                if time.strftime("%R", time.localtime()) == "16:30":
                    funcsms.send_telegram("Dear", "administrator", "you've done your part today, while I'm prepping!")
                    time.sleep(60)

            # RUN THE VACUUM SCRIPT
            if datetime.datetime.now() >= funcconf.d_run_vacuum:

                # SET DATE AND TIME FOR NEXT RUN
                if time.strftime("%R", time.localtime()) <= "15:55":
                    funcconf.d_run_vacuum = datetime.datetime.strptime(funcdate.today() +
                                                                       " 16:00:00",
                                                                       "%Y-%m-%d %H:%M:%S")
                else:
                    funcconf.d_run_vacuum = datetime.datetime.strptime(funcdate.today() +
                                                                       " 16:00:00",
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

            # SLEEPER
            time.sleep(i_sleep)


class RunLarge(Thread):

    def run(self):
        """
        Thread to execute the large schedule.
        :return:
        """

        # IMPORT PYTHON PACKAGES
        import datetime
        import time

        # IMPORT OWN MODULES
        from _my_modules import funcdate
        from _my_modules import funcfile
        from _my_modules import funcmail
        from _my_modules import funcsms
        from _my_modules import funcsys

        # IMPORT SCRIPTS
        import A001_oracle_to_sqlite

        # DECLARE VARIABLES
        l_clock: bool = False  # Display the local clock
        i_sleep: int = 60  # Sleeping time in seconds

        # SEND MESSAGE TO INDICATE START OF LARGE THREAD
        if funcconf.l_mess_project:
            funcsms.send_telegram("Dear", "administrator", "large schedule thread loaded!")

        # DO UNTIL GLOBAL l_run_project IS FALSE
        while funcconf.l_run_project:

            # DISPLAY THE LOCAL TIME
            if l_clock:
                print("LARGE thread" + time.strftime("%T", time.localtime()))

            # RUN THE LARGE SCRIPT ON SCHEDULE
            if datetime.datetime.now() >= funcconf.d_run_large:

                # SET DATE AND TIME FOR NEXT RUN
                if time.strftime("%R", time.localtime()) <= "17:55":
                    funcconf.d_run_large = datetime.datetime.strptime(funcdate.today() +
                                                                      ' 18:00:00',
                                                                      "%Y-%m-%d %H:%M:%S")
                else:
                    funcconf.d_run_large = datetime.datetime.strptime(funcdate.today() +
                                                                      " 18:00:00",
                                                                      "%Y-%m-%d %H:%M:%S") \
                                           + datetime.timedelta(days=1)

                # MESSAGES
                if funcconf.l_mess_project:
                    funcsms.send_telegram('', 'administrator',
                                          '<b>Large</b> schedule started.')

                # IMPORT PEOPLE
                s_project: str = "A001_oracle_to_sqlite(people)"
                # RUN ONLY MONDAYS TO FRIDAYS
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
                        funcsys.ErrMessage(err, funcconf.l_mail_project,
                                           "NWUIACA:Fail:" + s_project,
                                           "NWUIACA: Fail: " + s_project)
                else:
                    print("ORACLE to SQLITE " + s_project + " do not run on Saturdays and Sundays")
                    funcfile.writelog("SCRIPT: " + s_project.upper() + ": DO NOT RUN ON SATURDAYS AND SUNDAYS")

                # IMPORT VSS
                s_project: str = "A001_oracle_to_sqlite(vss)"
                # RUN ONLY MONDAYS TO FRIDAYS
                if funcdate.today_dayname() in "MonTueWedThuFri":
                    try:
                        A001_oracle_to_sqlite.oracle_to_sqlite("000b_Table - vss.csv", "VSS")
                        if funcconf.l_mail_project:
                            funcmail.Mail('std_success_gmail',
                                          'NWUIACA:Success:' + s_project,
                                          'NWUIACA: Success: ' + s_project)
                    except Exception as err:
                        # DISABLE PEOPLE TESTS
                        funcconf.l_run_vss_test = False
                        funcsys.ErrMessage(err, funcconf.l_mail_project,
                                           "NWUIACA:Fail:" + s_project,
                                           "NWUIACA: Fail: " + s_project)
                else:
                    print("ORACLE to SQLITE " + s_project + " do not run on Saturdays and Sundays")
                    funcfile.writelog("SCRIPT: " + s_project.upper() + ": DO NOT RUN ON SATURDAYS AND SUNDAYS")

                # SEND MAIL TO INDICATE THE SUCCESSFUL COMPLETION OF LARGE SCHEDULE
                if funcconf.l_mail_project:
                    funcmail.Mail('std_success_gmail', 'Python:Success:Finished:LargeSchedule',
                                  'NWUIAPython: Success: Finished : Large schedule')
                    funcmail.Mail("python_log")

                if funcconf.l_mess_project:
                    funcsms.send_telegram('', 'administrator',
                                          '<b>Large</b> schedule finished.')

            # SLEEPER
            time.sleep(i_sleep)


class RunSmall(Thread):

    def run(self):
        """
        Thread to execute the small schedule.
        :return:
        """

        # IMPORT PYTHON PACKAGES
        import datetime
        import time

        # IMPORT OWN MODULES
        from _my_modules import funcdate
        from _my_modules import funcfile
        from _my_modules import funcmail
        from _my_modules import funcsms
        from _my_modules import funcsys

        # IMPORT SCRIPTS
        import A001_oracle_to_sqlite

        # DECLARE VARIABLES
        l_clock: bool = False  # Display the local clock
        i_sleep: int = 60  # Sleeping time in seconds

        # SEND MESSAGE TO INDICATE START OF SMALL THREAD
        if funcconf.l_mess_project:
            funcsms.send_telegram("Dear", "administrator", "small schedule thread loaded!")

        # DO UNTIL GLOBAL l_run_project IS FALSE
        while funcconf.l_run_project:

            # DISPLAY THE LOCAL TIME
            if l_clock:
                print("SMALL thread" + time.strftime("%T", time.localtime()))

            # RUN THE SMALL SCHEDULE
            if datetime.datetime.now() >= funcconf.d_run_small:

                # SET DATE AND TIME FOR NEXT RUN
                if time.strftime("%R", time.localtime()) <= "23:59":
                    funcconf.d_run_small = datetime.datetime.strptime(funcdate.today() + " 23:00:00",
                                                                      "%Y-%m-%d %H:%M:%S") + \
                                           datetime.timedelta(hours=3)
                else:
                    funcconf.d_run_small = datetime.datetime.strptime(funcdate.today() + " 23:00:00",
                                                                      "%Y-%m-%d %H:%M:%S") + \
                                           datetime.timedelta(days=1, hours=3)

                # MESSAGES
                if funcconf.l_mess_project:
                    funcsms.send_telegram('', 'administrator',
                                          '<b>Small</b> schedule started.')

                # IMPORT KFS DATA IN THE SMALL SCHEDULE
                s_project: str = "A001_oracle_to_sqlite(kfs)"
                # RUN ONLY MONDAYS TO FRIDAYS
                if funcdate.today_dayname() in "TueWedThuFriSat":
                    try:
                        A001_oracle_to_sqlite.oracle_to_sqlite("000b_Table - temp.csv", "KFS")
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
                    funcfile.writelog("SCRIPT: " + s_project.upper() + ": DO NOT RUN ON SUNDAYS AND MONDAYS")

                # SET THE TEST SCHEDULE TWO MINUTES AFTER COMPLETING
                funcconf.d_run_test = datetime.datetime.now() + datetime.timedelta(minutes=2)

                # SEND MAIL TO INDICATE THE SUCCESSFUL COMPLETION OF SMALL SCHEDULE
                if funcconf.l_mail_project:
                    funcmail.Mail('std_success_gmail', 'Python:Success:Finished:SmallSchedule',
                                  'NWUIAPython: Success: Finished : Small schedule')

                if funcconf.l_mess_project:
                    funcsms.send_telegram('', 'administrator',
                                          '<b>Small</b> schedule finished.')

            # SLEEPER
            time.sleep(i_sleep)


class RunTest(Thread):

    def run(self):
        """
        Thread to execute the test schedule.
        :return:
        """

        # IMPORT PYTHON PACKAGES
        import datetime
        import time

        # IMPORT OWN MODULES
        from _my_modules import funcdate
        from _my_modules import funcfile
        from _my_modules import funcmail
        from _my_modules import funcsms
        from _my_modules import funcsys

        # IMPORT SCRIPTS
        import A002_log

        # DECLARE VARIABLES
        l_clock: bool = False  # Display the local clock
        i_sleep: int = 60  # Sleeping time in seconds

        # SEND MESSAGE TO INDICATE START OF TEST THREAD
        if funcconf.l_mess_project:
            funcsms.send_telegram("Dear", "administrator", "test schedule thread loaded!")

        # DO UNTIL GLOBAL l_run_project IS FALSE
        while funcconf.l_run_project:

            # DISPLAY THE LOCAL TIME
            if l_clock:
                print("TEST thread" + time.strftime("%T", time.localtime()))

            # RUN THE TEST SCHEDULE
            if datetime.datetime.now() >= funcconf.d_run_test:

                # SET DATE AND TIME FOR NEXT RUN
                if time.strftime("%R", time.localtime()) <= "23:59":
                    funcconf.d_run_test = datetime.datetime.strptime(funcdate.today() + " 23:00:00",
                                                                     "%Y-%m-%d %H:%M:%S") + \
                                          datetime.timedelta(hours=5)
                else:
                    funcconf.d_run_test = datetime.datetime.strptime(funcdate.today() + " 23:00:00",
                                                                     "%Y-%m-%d %H:%M:%S") + \
                                          datetime.timedelta(days=1, hours=5)

                # MESSAGES
                if funcconf.l_mess_project:
                    funcsms.send_telegram('', 'administrator',
                                          '<b>Test</b> schedule started.')

                # PUT SCRIPTS HERE
                s_project: str = "A002_log"
                # RUN ONLY TWENTY FOUR SEVEN
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
                    funcfile.writelog("SCRIPT: " + s_project.upper() + ": DO NOT RUN ON SUNDAYS AND MONDAYS")

                # SEND MAIL TO INDICATE THE SUCCESSFUL COMPLETION OF TEST SCHEDULE
                if funcconf.l_mail_project:
                    funcmail.Mail('std_success_gmail', 'Python:Success:Finished:TestSchedule',
                                  'NWUIAPython: Success: Finished : Test schedule')
                    funcmail.Mail("python_log")

                if funcconf.l_mess_project:
                    funcsms.send_telegram('', 'administrator',
                                          '<b>Test</b> schedule finished.')

            # SLEEPER
            time.sleep(i_sleep)


if __name__ == '__main__':
    from _my_modules import funcsys

    try:
        main()
    except Exception as e:
        funcsys.ErrMessage(e)
