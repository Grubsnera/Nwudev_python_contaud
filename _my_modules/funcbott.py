"""
Bot commands
Test
"""
import datetime
import logging
from _my_modules import funcconf
from _my_modules import funcdate

# ENABLE LOGGING
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

def hi(update, context):
    """Send a message when the command /help is issued."""
    update.message.reply_text("<b>Welcome to NWU Internal Audit</b>\n"
                              "\n"
                              "Get information you may need to help with your audit.\n"
                              "\n"
                              "<b>What information do you want?</b>\n"
                              "\n"
                              "Reply with a number, command (or emoji) at any time to get the latest information on the topic:\n"
                              "\n"
                              "/set: Set a schedule 🕰\n"
                              "/stop: Stop me! 🕰", "HTML")


def help(update, context):
    """Send a message when the command /help is issued."""
    print('/HELP COMMAND')
    print('-------------')
    update.message.reply_text("Bot commands:\n"
                              "/help: This message\n"
                              "/set: Set a timer\n"
                              "/stop: Stop me in my tracks...")


def echo(update, context):
    """Echo the user message."""
    if update.message.text.lower() == "hi"\
        or update.message.text == "✋"\
        or update.message.text == "🤝":
        hi(update, hi)
    # elif update.message.text.lower() == "set"\
    #     or update.message.text == "🕰":
    #     msg = {'args': ['?']}
    #     set_schedule(update, msg)
    else:
        update.message.reply_text("I'm sorry, I do not understand that command!")


def set_schedule(update, context):
    """Add a job to the queue."""

    try:

        # args[0] set what?
        s_type = context.args[0]
        if s_type == "large":
            update.message.reply_text('Setting up <b>evening (large)</b> data schedule!', 'HTML')
        elif s_type == "test":
            update.message.reply_text('Setting up <b>morning</b> data and test schedule!', 'HTML')
        elif s_type == "vacuum":
            update.message.reply_text('Setting up <b>vacuuming</b> of data tables!', 'HTML')
        else:
            update.message.reply_text('❗️Usage: /set type year month day hour minute:\n'
                                      'Where:\n'
                                      'type = <b>vacuum</b> Vacuum schedule to optimize data\n'
                                      'type = <b>large</b> Evening schedule to run large data\n'
                                      'type = <b>test</b> Morning schedule to run tests\n'
                                      'year = This <b>year</b> or next year format YYYY\n'
                                      'month = <b>Month</b> to run schedule format -m\n'
                                      'day = <b>Day</b> to run schedule format -d\n'
                                      'hour = <b>Hour</b> to run schedule format -H\n'
                                      'minute = <b>Minute</b> to run schedule format -M\n'
                                      '- Please allow at least 5 minutes. 🤔\n'
                                      '- and we cannot go back in time. 😜', 'HTML')
            return

        # args should have values
        if not context.args[1] or\
                not context.args[2] or\
                not context.args[3] or\
                not context.args[4] or\
                not context.args[5]:
            update.message.reply_text('❗️Usage: /set <type> <year> <month> <day> <hour> <minute>')

        # args[1] should contain the schedule year
        i_year = int(context.args[1])
        if i_year < int(funcdate.cur_year()) or i_year > int(funcdate.cur_year()) + 1:
            update.message.reply_text('❗️Sorry, schedule this or next year!', 'HTML')
            return

        # args[2] should contain the schedule month
        i_month = int(context.args[2])
        if i_month < 1 or i_month > 12:
            update.message.reply_text('❗️Sorry, month 1 to 12 only!', 'HTML')
            return

        # args[3] should contain the schedule day
        i_day = int(context.args[3])
        if i_day < 1 or i_day > 31:
            update.message.reply_text('❗️Sorry, day 1 to 31 only!', 'HTML')
            return

        # args[4] should contain the schedule hour
        i_hour = int(context.args[4])
        if i_hour < 0 or i_hour > 24:
            update.message.reply_text('❗️Sorry, hour 0 to 24 only!', 'HTML')
            return

        # args[5] should contain the schedule minute
        i_minute = int(context.args[5])
        if i_minute < 0 or i_minute > 59:
            update.message.reply_text('❗️Sorry, minute 0 to 59 only!', 'HTML')
            return

        if datetime.datetime(i_year, i_month, i_day, i_hour, i_minute, 0) < datetime.datetime.now():
            update.message.reply_text('❗️Sorry, we cannot go back in time!', 'HTML')
            return

        # global d_run_large
        if s_type == "vacuum":
            funcconf.d_run_vacuum = datetime.datetime(i_year, i_month, i_day, i_hour, i_minute, 0)

        if s_type == "large":
            funcconf.d_run_large = datetime.datetime(i_year, i_month, i_day, i_hour, i_minute, 0)

        if s_type == "test":
            funcconf.d_run_test = datetime.datetime(i_year, i_month, i_day, i_hour, i_minute, 0)

        update.message.reply_text('Scheduler successfully set!', 'HTML')

    except (IndexError, ValueError):
            update.message.reply_text('🕰 Schedule currently set to:\n'
                                      '<b>vacuum</b> = ' + datetime.datetime.strftime(funcconf.d_run_vacuum, '%a %Y-%m-%d') +
                                      ' ' + datetime.datetime.strftime(funcconf.d_run_vacuum, '%H:%M:%S') + '\n'
                                      '<b>large</b> = ' + datetime.datetime.strftime(funcconf.d_run_large, '%a %Y-%m-%d') +
                                      ' ' + datetime.datetime.strftime(funcconf.d_run_large, '%H:%M:%S') + '\n'
                                      '<b>test</b> = ' + datetime.datetime.strftime(funcconf.d_run_test, '%a %Y-%m-%d') +
                                      ' ' + datetime.datetime.strftime(funcconf.d_run_test, '%H:%M:%S') + '\n', 'HTML')


def stop(update, context):
    """Send a message when the command /start is issued."""
    update.message.reply_text('Stopping me!')
    funcconf.l_run_project = False
    return exit(SystemExit)


def error(update, context):
    """Log Errors caused by Updates."""
    logger.warning('Update "%s" caused error "%s"', update, context.error)
