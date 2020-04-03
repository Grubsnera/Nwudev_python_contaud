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
from _my_modules import funcsys

# SET TO TRUE FOR ACTIVE NWU USE OR COMMENT OUT
# funcconf.l_tel_use_nwu = False

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
    from _my_modules import funcsms

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
    print("Starting vacuum thread")

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
        import A003_table_vacuum

        # DECLARE VARIABLES
        l_clock: bool = True  # Display the local clock
        i_sleep: int = 1  # Sleeping time in seconds

        # DO UNTIL GLOCAL l_run_project IS FALSE
        while funcconf.l_run_project:

            # DISPLAY THE LOCAL TIME
            if l_clock:
                print(time.strftime("%T", time.localtime()))

            # SEND MESSAGE TO INDICATE START OF WORKING DAY
            if time.strftime("%R", time.localtime()) == "07:45":
                funcsms.send_telegram("Dear", "administrator", "your working day started, and I'm up and running!")
                time.sleep(60)

            # SEND MESSAGE TO INDICATE LUNCH TIME
            if time.strftime("%R", time.localtime()) == "12:55":
                funcsms.send_telegram("Dear", "administrator", "how about going for a walk, while I'm keeping up!")
                time.sleep(60)

            # SEND MESSAGE TO INDICATE WORKING DAY END
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
                    funcmail.Mail('std_success_gmail',
                                  'NWUIACA:Success:A003_table_vacuum',
                                  'NWUIACA: Success: A003_table_vacuum')

                except Exception as err:

                    funcsys.ErrMessage(err, True,
                                       "NWUIACA:Fail:A003_table_vacuum",
                                       "NWUIACA: Fail: A003_table_vacuum")

            # SLEEPER
            time.sleep(i_sleep)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        funcsys.ErrMessage(e)
