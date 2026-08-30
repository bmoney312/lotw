import os
import sys
import json
import pymysql
import logging
import datetime
from lotw import validate_field, validate_key, get_player_info
from lotw import get_current_pick, get_kickoff_time, get_line, get_current_year
from lotw import build_html_message, build_html_response, send_email
from lotw import formatted_line, response

# global variables
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def submit_registration(conn, player_id, registration, year):
    """
    Update registartion status for player player_id.
    """

    # update players table with new registration value
    try:
        with conn.cursor() as cur:
            if registration == True:
                sql = "UPDATE `Players` SET `" + str(year) + "_registration` = 1 WHERE `player_id` = %s"
            elif registration == False:
                sql = "UPDATE `Players` SET `" + str(year) + "_registration` = 0 WHERE `player_id` = %s"
            else:
                raise ValueError("Invalid registartion value {} for player {}".format(registration, player_id))

            logger.debug("submit_registration() SQL: {}".format(sql))
            cur.execute(sql, (player_id, ))
            conn.commit()
    except Exception as e:
        return (False, "Error updating database: {}".format(str(e)))

    if registration == True:
        message = "Your registration was updated successfully! You are signed up for LOTW this season!"
    else:
        message = "You will not be registered for LOTW this season. Hope to see you back in the future."

    return (True, message)



def lambda_handler(event, context):
    """
    process_registration.py

    Process LOTW sign up reqeusts 
    """
    
    logger.info("Received event: " + json.dumps(event, indent=2))

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
    
    query_string_params = event.get('queryStringParameters')

    if query_string_params is not None:
        # HTTP GET from API Gateway
        player_id = query_string_params.get('id')
        registration = query_string_params.get('registration')
    else:
        # Lambda test event
        player_id = event.get('id')
        registration = event.get('registration')

    logger.debug("Validating input fields")

    # validate pick is valid team
    if not validate_field(conn, player_id, 'player_id', 'Players'):
        return response(400, 'text/html', build_html_response("invalid player {}".format(player_id)))

    player_id = int(player_id)

    if registration is None:
        return response(400, 'text/html', build_html_response("invalid null registration value"))

    if registration == "true":
        registration = True
    elif registration == "false":
        registration = False
    else:
        return response(400, 'text/html', build_html_response("invalid value for registration {}".format(registration)))

    logger.debug("calling submit_registration()")

    current_year = get_current_year()
    (res, message) = submit_registration(conn, player_id, registration, current_year)

    payment_info ="""
The league fee is $50. <b>The fee is due before the season starts.</b> You will receive the weekly lines when payment is received. See payment information below.<br>
<br>
<b>Check:</b><br>
Brendan Connell<br>
20032 11th Place W<br>
Lynnwood, WA 98036<br>
<br>
<b>PayPal:</b> <a href=https://paypal.me/BrendanConnell>paypal.me/BrendanConnell</a>
<br><br>
<b>Venmo:</b> <a href=https://venmo.com/bmoney312>venmo.com/bmoney312</a>
<br>
"""
    # send email to confirm picks that were recorded successfully
    if res is True and registration == True:
        logger.debug("sending email to player_id {}".format(player_id))
        # initialize variables
        mail_username = os.environ['mail_username']
        mail_password = os.environ['mail_password']
        mail_host = os.environ['mail_host']
        mail_port = os.environ['mail_port']
        mail_from = '"Brendan Connell" <bmoney312@gmail.com>'
        (player_email, player_first_name, player_last_name) = get_player_info(conn, player_id)
        logger.debug("Player info: {} {} {}".format(player_first_name, player_last_name, player_email))

        mail_to = (player_email, 'bmoney312@gmail.com')
        mail_subject = "lotw: registration confirmation and payment information"
        mail_body = build_html_message("Hi {},<br><br>".format(player_first_name) + message + payment_info + "<br><br>Thanks, -BMC")
        email_result = send_email(mail_host, mail_port, mail_username, mail_password, mail_subject, mail_body, mail_to, mail_from)

        if email_result is True:
            logger.debug("Email sent successfully")
        else:
            logger.debug("Email failed")

    # close database connection
    conn.close()

    if res == False:
        return response(400, 'text/html', build_html_response(message))

    # return result
    return response(200, 'text/html', build_html_response(message))

