import os
import sys
import json
import pymysql
import logging
import datetime
from time import sleep
from lotw import get_current_week, get_commish_message, get_player, get_all_paid_players
from lotw import build_html, response, build_html_head, send_email, smtp_connect, smtp_send

# global variables
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    Email LOTW players with league announcement
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

    player_id = os.environ.get('player_id')

    if request_type == "test":
        players = get_player(conn, '0000000001')
    elif request_type == "manual_run":
        if player_id is None:
            players = get_all_paid_players(conn)
        else:
            players = get_player(conn, int(player_id))
    else:
        logger.error("Invalid request type {}".format(request_type))
        sys.exit()

    logger.info("Request type is {}".format(request_type))
    logger.debug("Players {}".format(players))

    # determine message
    message_id = os.environ.get('message_id')

    # if week is not provided
    if message_id is None:
        logger.error("ERROR: Unable to determine message_id!")
        sys.exit()

    logger.info("Current message_id set to {}".format(message_id))
    logger.info("Current time is {}".format(datetime.datetime.now()))

    result = get_commish_message(conn, message_id)

    if result is None:
        logger.error("Unexpected missing value for commish message")
        sys.exit()

    (mail_subject, commish_message) = result

    if commish_message is None:
        logger.error("Unexpected missing value for commish message")
        sys.exit()

    if mail_subject is None:
        logger.error("Unexpected missing value for e-mail subject")
        sys.exit()

    smtp_relay = smtp_connect(mail_host, mail_port, mail_username, mail_password)

    if smtp_relay is None:
        logger.error("Error establishing SMTP connection with {}".format(mail_host))
        sys.exit()

    # email standings to each player
    for row in players:
        (player_id, player_email, last_name, first_name, titles, is_rookie) = row
        logger.info("Working on player {} {} {} {}".format(player_id, first_name, last_name, player_email))

        # build message body
        message = "<body><p>\nHello {},<br></p>".format(first_name)

        # build email body for this player
        mail_body = build_html_head() + message + commish_message + "<br></body></html>"

        mail_to = (player_email, 'bmoney312@gmail.com')
        email_result = smtp_send(smtp_relay, mail_subject, mail_body, mail_to, mail_from)

        if email_result is True:
            logger.info("Email sent successfully to {} {}".format(player_id, player_email))
        else:
            logger.info("Email to {} {} failed".format(player_id, player_email))
            logger.info("Sleeping for 15 seconds before retry...")
            smtp_relay.close()
            sleep(15)

            # connect to SMTP relay
            smtp_relay = None
            smtp_relay = smtp_connect(mail_host, mail_port, mail_username, mail_password)

            if smtp_relay is None:
                logger.error("Error establishing SMTP connection with {}".format(mail_host))
                conn.close()
                return response(200, 'text/html', build_html("Send failed for some players ({}).".format(player_id)))

            email_result = smtp_send(smtp_relay, mail_subject, mail_body, mail_to, mail_from)
            if email_result is True:
                logger.info("Retry email sent successfully to player {} {}".format(player_id, player_email))
            else:
                logger.error("Retry email failed to player {} {}, exiting".format(player_id, player_email))
                smtp_relay.close()
                conn.close()
                return response(504, 'text/html', build_html("Send failed for some players ({}).".format(player_id)))

    # close database connection
    conn.close()

    # close SMTP connection
    smtp_relay.close()

    # return result
    return response(200, 'text/html', build_html("Commish message message_id {} sent successfully.".format(message_id)))

