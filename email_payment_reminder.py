import os
import sys
import json
import pymysql
import logging
from time import sleep
from lotw import get_current_year, build_html, build_html_head
from lotw import response, smtp_connect, smtp_send

# global variables
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_unpaid_registered_players(conn, year, player_id=None):
    """
    Return rows in LOTW Players database who are registered for the
    specified year but have not yet paid the entry fee.
    If player_id is provided, filters for that specific player.
    """
    with conn.cursor() as cur:
        select_statement = """
            SELECT `player_id`, `email`, `first_name`, `last_name`
            FROM Players
            WHERE `{}_registration` = 1
            AND (`{}_paid` IS NULL OR `{}_paid` = 0)
        """.format(year, year, year)

        if player_id is not None:
            select_statement += " AND `player_id` = %s"
            logger.debug("get_unpaid_registered_players(): {} with player_id {}".format(select_statement, player_id))
            cur.execute(select_statement, (player_id,))
        else:
            logger.debug("get_unpaid_registered_players(): {}".format(select_statement))
            cur.execute(select_statement)

        result = cur.fetchall()
        return result


def lambda_handler(event, context):
    """
    Email LOTW payment reminders to players who registered but have not paid.
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
    db_name = os.environ['db_name']

    logger.info("Connecting to MySQL database {}".format(db_endpoint))

    try:
        conn = pymysql.connect(host=db_endpoint, port=db_port,
                               user=db_username, passwd=db_password,
                               db=db_name, connect_timeout=5)
    except Exception as e:
        logger.error("ERROR: Unexpected error: Could not connect to MySQL database - {}".format(str(e)))
        sys.exit()

    logger.info("SUCCESS: Connection to MySQL database succeeded")

    # get unpaid players
    current_year = get_current_year()
    player_id = os.environ.get('player_id')

    if request_type == "test":
        # players = get_unpaid_registered_players(conn, current_year, int(1))
        players = [
            (1, "bmoney312@yahoo.com", "Brendan", "Connell"),
        ]
    elif request_type == "manual_run":
        if player_id is None:
            players = get_unpaid_registered_players(conn, current_year)
        else:
            players = get_unpaid_registered_players(conn, current_year, int(player_id))
    else:
        logger.error("Invalid request type {}".format(request_type))
        sys.exit()

    logger.info("Request type is {}".format(request_type))

    if not players:
        conn.close()
        return response(200, 'text/html', build_html("No unpaid registered players found. Everyone is paid up!"))
    else:
        logger.info("Found {} unpaid registered players for the {} season.".format(len(players), current_year))
        logger.info("Players {}".format(players))

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

    smtp_relay = smtp_connect(mail_host, mail_port, mail_username, mail_password)

    if smtp_relay is None:
        logger.error("Error establishing SMTP connection with {}".format(mail_host))
        conn.close()
        sys.exit()

    payment_info = """
The league fee is $50. <b>The fee is due before the season starts.</b> You will begin receiving the weekly lines as soon as your payment is processed. See payment information below. Please reply to this email if you have questions or if this was sent in error.<br>
<br>
<b>Check:</b><br>
Brendan Connell<br>
20032 11th Place W<br>
Lynnwood, WA 98036<br>
<br>
<b>PayPal:</b> <a href="https://paypal.me/BrendanConnell">paypal.me/BrendanConnell</a>
<br><br>
<b>Venmo:</b> <a href="https://venmo.com/bmoney312">venmo.com/bmoney312</a>
<br>
"""

    for row in players:
        (player_id, player_email, first_name, last_name) = row
        logger.info("Working on player {} {} {} {}".format(player_id, first_name, last_name, player_email))

        # build message body
        message = "<body>\n<p>Hello {},<br><br>".format(first_name)
        message += "This is a friendly reminder that you are registered for the {} Lock of the Week season, but I haven't received your entry fee yet.<br><br>".format(current_year)
        message += payment_info + "<br>Thanks,<br>-BMC</p></body></html>"

        mail_body = build_html_head() + message
        mail_to = (player_email, 'bmoney312@gmail.com')
        mail_subject = "lotw: payment reminder for the {} season".format(current_year)

        # --- Send email with retry logic ---
        email_sent_successfully = False
        for attempt in range(MAX_RETRIES):
            email_result = smtp_send(smtp_relay, mail_subject, mail_body, mail_to, mail_from)

            if email_result is True:
                logger.info("Email sent successfully to player {} {} on attempt {}".format(player_id, player_email, attempt + 1))
                email_sent_successfully = True
                break
            else:
                logger.error("Email failed to player {} {} on attempt {}".format(player_id, player_email, attempt + 1))
                if attempt < MAX_RETRIES:
                    logger.info("Sleeping for {} seconds before retry...".format(RETRY_SLEEP_SECONDS))
                    smtp_relay.close()
                    sleep(RETRY_SLEEP_SECONDS)

                    # Reconnect to SMTP relay
                    smtp_relay = None
                    smtp_relay = smtp_connect(mail_host, mail_port, mail_username, mail_password)

                    if smtp_relay is None:
                        logger.error("Error re-establishing SMTP connection with {}. Stopping retries for this player.".format(mail_host))
                        break
                else:
                    logger.error("All {} retry attempts failed for player {} {}".format(MAX_RETRIES, player_id, player_email))

        if not email_sent_successfully:
            logger.error("Aborting email send for player {} {} after all retries.".format(player_id, player_email))

            if smtp_relay is None:
                logger.error("SMTP connection is dead.")
            else:
                logger.info("Closing connection to SMTP relay.")
                smtp_relay.close()

            conn.close()
            raise RuntimeError("Payment reminder send failed for player {} {} after {} attempts. Aborting.".format(player_id, player_email, MAX_RETRIES))

        sleep(2)

    conn.close()
    smtp_relay.close()

    return response(200, 'text/html', build_html("Payment reminder emails sent successfully."))
