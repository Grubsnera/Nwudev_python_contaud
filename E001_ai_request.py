"""
Script to query chatGPT
Author: Albert B Janse van Rensburg (NWU:21162395)
Created: 13 April 2023
"""

# IMPORT OWN MODULES
from _my_modules import funcconf
from _my_modules import funcsys

# INDEX
"""
ENVIRONMENT
COMPLETION REQUEST
END OF SCRIPT
"""

# VARIABLES
s_script: str = "E001_ai_request"


def ai_completion(s_prompt: str = '', s_name: str = '', i_token: int = 1024):
    """
    Script to query chatGPT completion
    :param s_prompt: The query.
    :param s_name: The name of the requester / recipient
    :param i_token: Maximum tokens (max < 4096)
    :return: str: The chatbot completion message
    """

    # IMPORT PYTHON MODULES
    import openai
    from datetime import datetime

    # IMPORT OWN MODULES
    from _my_modules import funcfile
    from _my_modules import funcsms
    # from _my_modules import funccsv
    # from _my_modules import funcdate
    # from _my_modules import funcmail
    # from _my_modules import funcstat
    # from _my_modules import funcoracle

    # DECLARE VARIABLES
    l_debug: bool = False

    """*************************************************************************
    ENVIRONMENT
    *************************************************************************"""
    if l_debug:
        print("ENVIRONMENT")

    # DECLARE VARIABLES
    s_function: str = 'ai_completion'
    s_description: str = "chatGPT completion request"
    l_mess: bool = funcconf.l_mess_project
    l_mess: bool = True
    l_mailed: bool = False

    # Exit function if no prompt
    if s_prompt == '':
        return 'No prompt given!'
    if s_name == '':
            s_name = 'Unknown'

    # LOG
    if l_debug:
        print(s_script.upper())
    funcfile.writelog("Now")
    funcfile.writelog("SCRIPT: " + s_script.upper())
    funcfile.writelog("-" * len("script: "+s_script))
    funcfile.writelog('FUNCTION: ' + s_function.upper())
    funcfile.writelog("%t " + s_description + " by " + s_name)

    # MESSAGE
    if l_mess:
        funcsms.send_telegram("", "administrator", "<b>" + s_script.upper() + "</b>")

    """*************************************************************************
    COMPLETION REQUEST
    *************************************************************************"""
    if l_debug:
        print("COMPLETION REQUEST")

    # Initiate chatGPT
    openai.organization = funcconf.s_gpt_org
    openai.api_key = funcconf.s_gpt_api

    # Feed the prompt
    response = openai.Completion.create(
        model="text-davinci-003",
        prompt=s_prompt,
        temperature=0.7,
        max_tokens=i_token,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0
    )

    # Print the response
    if l_debug:
        print(response)

    # Build response text
    response_text: str = response["choices"][0]["text"]
    response_text = response_text.replace('\n', '')

    # Print some response parameters
    if l_debug:
        print('       Prompt: ' + s_prompt)
        print('Finish reason: ' + response["choices"][0]["finish_reason"])
        print('        Index: ' + str(response["choices"][0]["index"]))
        print('      Created: ' + str(response["created"]))
        print('  Response_id: ' + response["id"])

    # Log
    funcfile.writelog("MODEL: " + response["model"])
    funcfile.writelog("OBJECT: " + response["object"])
    funcfile.writelog("PROMPT: (" + str(response["usage"]["prompt_tokens"]) + ") " + s_prompt)
    funcfile.writelog("RESPONSE: (" + str(response["usage"]["completion_tokens"]) + ") " + response_text)

    # MESSAGE
    if l_mess:
        funcsms.send_telegram("", "administrator", s_description + " by " + s_name)

    # Build the return message
    s_return_message: str = response_text

    """*****************************************************************************
    END OF SCRIPT
    *****************************************************************************"""
    funcfile.writelog("END OF SCRIPT")
    if l_debug:
        print("END OF SCRIPT")

    # CLOSE THE LOG WRITER
    funcfile.writelog("-" * len("completed: "+s_script))
    funcfile.writelog("COMPLETED: " + s_script.upper())

    return s_return_message[0:4096]


if __name__ == '__main__':
    try:
        s_return = ai_completion('What is the meaning of life?', 'Albert', 2056)
        print("RETURN: " + s_return)
        print("LENGTH: " + str(len(s_return)))
    except Exception as e:
        funcsys.ErrMessage(e, funcconf.l_mess_project, s_script, s_script)
