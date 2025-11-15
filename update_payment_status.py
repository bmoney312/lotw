import os
import sys
import json
import pymysql
import logging
import datetime
from time import sleep
from lotw import get_current_year, get_player, get_current_year
from lotw import build_html, build_html_head, response, validate_field, smtp_send, smtp_connect

# Mark a player paid

# global variables
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def mark_player_paid(conn, player_id, year):
    """
    Update database to reflect player is paid for year
    """
    logger.info("Validating player {} before update".format(player_id))
    result = validate_field(conn, str(player_id), str("player_id"), str("Players"))
    if result is not True:
        return (False, "Invalid player id {}".format(player_id))

    logger.info("Marking player {} as paid for {}".format(player_id, year))
    try:
        with conn.cursor() as cur:
            sql = "UPDATE `Players` SET `" + str(year) + "_paid` = 1 WHERE `player_id` = %s"
            logger.debug("mark_player_paid(): {}".format(sql))
            cur.execute(sql, (int(player_id),))
            conn.commit()
    except Exception as e:
        logger.error("Error updating database: {}".format(str(e)))
        raise

    logger.info("Successfully updated Players table")
    return (True, "Marked player {} as paid for {}".format(player_id, year))


def lambda_handler(event, context):
    """
    Mark player as paid for current year
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
    current_year = get_current_year()
    
    if request_type == "Scheduled Event":
        logger.debug("Scheduled Event")
        logger.error("Cannot run as Scheduled Event, exiting")
        sys.exit()
    elif request_type == "test":
        logger.debug("test")
        players = get_player(conn, int(1))
    elif request_type == "manual_run":
        logger.debug("manual_run")
        players = get_player(conn, int(player_id))
    else:
        logger.error("Invalid request type {}".format(request_type))
        sys.exit()

    logger.info("Request type is {}".format(request_type))
    logger.info("Current year is {}".format(current_year))

    smtp_relay = smtp_connect(mail_host, mail_port, mail_username, mail_password)

    if smtp_relay is None:
        logger.error("Error establishing SMTP connection with {}".format(mail_host))
        sys.exit()

    for row in players:
        (player_id, player_email, last_name, first_name, titles, is_rookie) = row
        logger.info("Working on player {} {} {} {}".format(player_id, first_name, last_name, player_email))
        logger.info("Marking player {} {} {} as paid".format(player_id, first_name, last_name))
        (result, message) = mark_player_paid(conn, int(player_id), int(current_year))

        if result is not True:
            return response(200, 'text/html', build_html("Marking player {} {} {} failed".format(player_id, first_name, last_name)))

        # build message body
        message = "<body><p>\nHello {},<br><br>Your $50 Lock of the Week payment has been processed. You are officially registered. Good luck this season!<br><br>Thanks, -BMC</p>".format(first_name)

        # build email body for this player
        mail_body = build_html_head() + message + "<br></body></html>"
        mail_subject = "lotw: your payment was received"

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

    # return result
    return response(200, 'text/html', build_html("Successfully marked player {} {} {} as paid for {}".format(player_id, first_name, last_name, current_year)))

