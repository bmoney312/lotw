import os
import sys
import json
import pymysql
import logging
import datetime
from time import sleep
from lotw import get_player_reg, get_current_year, get_past_registered_players
from lotw import build_html, build_html_head, response, send_email, smtp_connect, smtp_send

# global variables
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def get_button_html(player_id):
    """
    print <button> and <form> html for submit and cancel buttons
    """
    html = """
 <form method="post" action="https://w95d9hh2z8.execute-api.us-west-2.amazonaws.com/prod/register" class="inline">
   <input type="hidden" name="id" value="{}">
   <button type="submit" name="registration" value="true" class="btn-primary">
 Yes, Sign Me Up!
   </button>
 </form>
  &nbsp;
 <form action="https://w95d9hh2z8.execute-api.us-west-2.amazonaws.com/prod/register" class="inline">
   <input type="hidden" name="id" value="{}">
   <button type="submit" name="registration" value="false" class="btn-default">
 No thanks
   </button>
 </form>
 """.format(player_id, player_id)

    return html
    

def build_email_head():
    """
    Build lines email head
    """

    html = """
<html>
<head>
   <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css">
    <style>
         .inline {
           display: inline;
         }

         .message {
           display: inline;
         }

         body {
             margin: 12;
             font-family: "Arial", "Helvetica", sans-serif;
         }
         h3 {
             padding: 2px;
         }
         table {
             border-collapse: collapse;
             border: 1px solid black;
         }
         th {
             border: 1px solid black;
             padding: 6px;
             text-align: left;
             background-color: lightgrey;
         }
         td {
             border: 1px solid black;
             padding: 6px;
             text-align: left;
         }
    </style>
</head>
"""
    return html


def lambda_handler(event, context):
    """
    Email LOTW sign up request for the upcoming season to each player
    """

    logger.info("Received event: " + json.dumps(event, indent=2))

    request_type = event.get('detail-type')
    if request_type is None:
        logger.error("Unable to determine request type")
        sys.exit()

    db_endpoint = os.environ['db_endpoint']
    db_port = int(os.environ['db_port'])
    db_username = os.environ['db_username']
    db_password = os.environ['db_password']
    db_name = db=os.environ['db_name']

    logger.info("Connecting to MySQL database {}".format(db_endpoint))

    try:
        conn = pymysql.connect(host=db_endpoint, port=db_port,
                                user=db_username, passwd=db_password,
                                db=db_name,connect_timeout=5)
    except:
        logger.error("ERROR: Unexpected error: Could not connect to MySQL database")
        sys.exit()

    logger.info("SUCCESS: Connection to MySQL database succeeded")
    
    # initialize variables
    mail_username = os.environ['mail_username']
    mail_password = os.environ['mail_password']
    mail_host = os.environ['mail_host']
    mail_port = os.environ['mail_port']
    mail_from = '"Brendan Connell" <bmoney312@gmail.com>'

    # --- Retry Configuration ---
    try:
        MAX_RETRIES = int(os.environ.get('SMTP_RETRIES', 5))
    except ValueError:
        MAX_RETRIES = 5

    try:
        RETRY_SLEEP_SECONDS = int(os.environ.get('SMTP_RETRY_SLEEP', 15))
    except ValueError:
        RETRY_SLEEP_SECONDS = 15
    # --- End Retry Configuration ---

    # determine current week
    player_id = os.environ.get('player_id')
    start_with_player_id = os.environ.get('start_with_player_id')

    if start_with_player_id:
        start_with_player_id = int(start_with_player_id)
        logger.info("Starting with player_id {}".format(start_with_player_id))

    players = []
    this_year = get_current_year()
    last_year = int(this_year) - 1
    if request_type == "Scheduled Event":
        players = get_past_registered_players(conn, last_year)
        logger.debug("email_registration(): all players: {}".format(players))
    elif request_type == "manual_run":
        if player_id is not None:
            players = get_player_reg(conn, int(player_id))
        else:
            players = get_past_registered_players(conn, last_year)
        logger.debug("email_registration(): all players: {}".format(players))
    elif request_type == "test":
        players = get_player_reg(conn, '0000000001')
    else:
        logger.error("Invalid request type {}".format(request_type))
        raise

    logger.info("Request type is {}".format(request_type))
    logger.debug("Players {}".format(players))

    smtp_relay = smtp_connect(mail_host, mail_port, mail_username, mail_password)

    if smtp_relay is None:
        logger.error("Error establishing SMTP connection with {}".format(mail_host))
        sys.exit()

    for row in players:
        (player_id, player_email, last_name, first_name, titles, is_rookie, registered) = row
        logger.info("Working on player {} {} {} {}".format(player_id, first_name, last_name, player_email))

        # skip players less than start_with_player_id
        # if start_with_player_id provided
        if start_with_player_id is not None and request_type != "test":
            if player_id < start_with_player_id:
                logger.info("Skipping player {} which is less than start_with_player_id {}".format(player_id, start_with_player_id))
                continue

        # skip email if player is already registered
        if registered is not None:
            if registered == 1:
                if request_type != "test":
                    logger.info("Skipping player {} {} who is already registered".format(player_id, player_email))
                    continue
                else:
                    logger.info("Sending email even though player {} is registered in {}".format(player_email, this_year))
            elif registered == 0:
                if request_type != "test":
                    logger.info("Skipping player {} {} who has declined invitation for {}".format(player_id, player_email, this_year))
                    continue
                else:
                    logger.info("Sending email even though player {} declined invitation for {}".format(player_email, this_year))

        # build message body
        message_head = "<body>\n<p>Hello {},<br><br>".format(first_name)
        registration_message = """
It's that time of the year again. You guessed it, it's time for Lock of the Week!  This is your invitation to participate.<br><br>

The rules have not changed.  Pick one game NFL game per week including the playoffs and Super Bowl.  The person with the best record at the end of the season wins.  The fee for the season is $50.<br><br>

Would you like to participate in LOTW this season?  Please click the link below to make your choice.<br><br>
<br>
<b><font size=+1><a href=https://w95d9hh2z8.execute-api.us-west-2.amazonaws.com/prod/register?id={}&registration=true>Yes! Sign me up!</a></font></b>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<a href=https://w95d9hh2z8.execute-api.us-west-2.amazonaws.com/prod/register?id={}&registration=false>No thanks</a>
        """.format(player_id, player_id)
        # build email body for this player
        message = message_head + registration_message
        mail_body = build_email_head() + message + "</body></html>"
        mail_to = (player_email, 'bmoney312@gmail.com')
        mail_subject = "lotw: are you in for the {}-{} season?".format(this_year, this_year + 1)

        # --- Send email with retry logic ---
        email_sent_successfully = False
        for attempt in range(MAX_RETRIES):
            email_result = smtp_send(smtp_relay, mail_subject, mail_body, mail_to, mail_from)

            if email_result is True:
                logger.info("Email sent successfully to player {} {} on attempt {}".format(player_id, player_email, attempt + 1))
                email_sent_successfully = True
                break # Exit retry loop on success
            else:
                logger.error("Email failed to player {} {} on attempt {}".format(player_id, player_email, attempt + 1))
                if attempt < MAX_RETRIES - 1:
                    logger.info("Sleeping for {} seconds before retry...".format(RETRY_SLEEP_SECONDS))
                    smtp_relay.close()
                    sleep(RETRY_SLEEP_SECONDS)

                    # Reconnect to SMTP relay
                    smtp_relay = None
                    smtp_relay = smtp_connect(mail_host, mail_port, mail_username, mail_password)

                    if smtp_relay is None:
                        logger.error("Error re-establishing SMTP connection with {}. Stopping retries for this player.".format(mail_host))
                        break # Break retry loop if reconnect fails
                else:
                    logger.error("All {} retry attempts failed for player {} {}".format(MAX_RETRIES, player_id, player_email))

        # If all retries failed, log and handle
        if not email_sent_successfully:
            logger.error("Aborting email send for player {} {} after all retries.".format(player_id, player_email))

            if smtp_relay is None:
                 logger.error("SMTP connection is dead.")

            # Close connections and exit with 504 error as requested
            conn.close()
            if smtp_relay:
                smtp_relay.close()
            return response(504, 'text/html', build_html("Registration send failed for player {} {} after {} attempts. Aborting.".format(player_id, player_email, MAX_RETRIES)))

    # close database connection
    conn.close()

    # close SMTP connection
    smtp_relay.close()

    # return result
    return response(200, 'text/html', build_html("registration emails sent successfully."))

