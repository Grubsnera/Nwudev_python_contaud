import datetime
from _my_modules import funcdate

def hi(update, context):
    """Send a message when the command /help is issued."""
    update.message.reply_text("<b>Welcome to NWU Internal Audit</b>\n"
                              "\n"
                              "Get information you may need to help with your audit.\n"
                              "\n"
                              "<b>What information do you want?</b>\n"
                              "\n"
                              "Reply with a number (or emoji) at any time to get the latest information on the topic:\n"
                              "\n"
                              "/set: Set a timer", 'HTML')


def help(update, context):
    """Send a message when the command /help is issued."""
    update.message.reply_text("Bot commands:\n"
                              "/help: This message\n"
                              "/set: Set a timer\n"
                              "/stop: Stop me in my tracks...")


def echo(update, context):
    """Echo the user message."""
    print(update)
    print(context)
    if update.message.text.lower() == "hi"\
        or update.message.text == "✋"\
        or update.message.text == "🤝":
        hi(update, hi)
    else:
        update.message.reply_text("I'm sorry, I do not understand that command!")


def alarm(context):
    """Send the alarm message."""
    job = context.job
    print(context.job)
    context.bot.send_message(job.context, text='Beep!')


def set_schedule(update, context):
    """Add a job to the queue."""
    global d_run_large
    global d_run_test
    chat_id = update.message.chat_id
    try:

        # args[0] set what?
        s_type = context.args[0]
        if s_type == "large":
            update.message.reply_text('Setting up <b>evening (large)</b> data schedule!', 'HTML')
        elif s_type == "test":
            update.message.reply_text('Setting up <b>morning</b> data and test schedule!', 'HTML')
        else:
            update.message.reply_text('❗️Sorry, <b>schedule type</b> unknown!\n'
                                      '\n'
                                      'Currently set to: ✅\n'
                                      '<b>large</b> = ' + datetime.datetime.strftime(d_run_large, '%c') + '\n'
                                      '<b>test</b> = ' + datetime.datetime.strftime(d_run_test, '%c') + '\n'
                                      '\n'
                                      '<b>Type</b> must be: ❎\n'
                                      '<b>large</b>: Large data evening schedule\n'
                                      '<b>test</b>: Test run morning schedule', 'HTML')
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

        # global d_run_large
        if s_type == "large":
            d_run_large = datetime.datetime(i_year, i_month, i_day, i_hour, i_minute, 0)

        if s_type == "test":
            d_run_test = datetime.datetime(i_year, i_month, i_day, i_hour, i_minute, 0)

        update.message.reply_text('Scheduler successfully set!', 'HTML')

    except (IndexError, ValueError):
        update.message.reply_text('❗️Usage: /set <type> <year> <month> <day> <hour> <minute>')


def unset(update, context):
    """Remove the job if the user changed their mind."""
    if 'job' not in context.chat_data:
        update.message.reply_text('You have no active timer')
        return

    job = context.chat_data['job']
    job.schedule_removal()
    del context.chat_data['job']

    update.message.reply_text('Timer successfully unset!')


def stop(update, context):
    """Send a message when the command /start is issued."""
    update.message.reply_text('Stopping me!')
    global l_run_project
    l_run_project = False
    return exit(SystemExit)


def error(update, context):
    """Log Errors caused by Updates."""
    logger.warning('Update "%s" caused error "%s"', update, context.error)
