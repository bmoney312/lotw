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
logger.setLevel(logging.INFO)


def submit_pick(conn, player_id, pick, week):
    """
    Submit new pick for player player_id in given week.
    
    Checks current time against any existing picks to confirm existing pick
    is not already locked in.
    
    Returns (result, pick, line, message)
    result: boolean value, 1 successful update, 0 failed update
    new_pick: team_id value for new pick if success, existing pick if failure
    new_line: integer value for new line if success, existing line if failure
    message: string with message of success or reason for failure
    """
    (current_pick_id, current_pick, current_line, current_pick_ats, is_locked_in) = get_current_pick(conn, player_id, week)
    
    # if pick is locked in do not allow change
    if is_locked_in:
        message = "Your week {} pick ".format(week) + current_pick + " " + formatted_line(current_line) + " is already locked in, " + "the game has started."
        logger.info("Player {} - {}".format(player_id, message))
        return (False, current_pick, current_line, message)

    # if pick is unchanged do not update picks table
    if current_pick == pick:
        message = "Your week {} pick was already <b>{} {}</b>".format(week, current_pick, formatted_line(current_line))
        logger.info("Player {} - {}".format(player_id, message))
        return (False, current_pick, current_line, message)

    # confirm pick is still avaiable
    time_now = datetime.datetime.now()
    kickoff_time = get_kickoff_time(conn, pick, week)
    logger.debug("submit_pick(): kickoff time for {} week {} is {}".format(pick, week, kickoff_time))

    if time_now >= kickoff_time:
        message = "Pick {} is off the board for week {}. Kick off time has passed ({}).".format(pick, week, kickoff_time)
        logger.info("Player {} - {}".format(player_id, message))
        return (False, current_pick, current_line, message)

    # confirm game has valid line (line must be posted, not null)
    line = get_line(conn, pick, week)
    if line is None:
        message = "Pick {} is off the board for week {}. Please select a different team.".format(pick, week)
        logger.info("Player {} - {}".format(player_id, message))
        return (False, current_pick, current_line, message)
    else:
        f_line = formatted_line(line)

    # update table with new pick
    try:
        with conn.cursor() as cur:
            sql = "INSERT INTO `Picks_" + str(get_current_year()) + "` (`player_id`, `week`, `pick`, `submit_time`, `lock_in_time`) VALUES (%s, %s, %s, %s, %s)"
            logger.debug("submit_pick(): ".format(sql))
            cur.execute(sql, (player_id, week, pick, time_now, kickoff_time))
            # set lock_in_time current pick to NULL which invalidates pick
            sql = "UPDATE `Picks_" + str(get_current_year()) + "` SET `lock_in_time` = NULL WHERE `pick_id` = %s"
            logger.debug("submit_pick(): ".format(sql))
            cur.execute(sql, (current_pick_id, ))
            conn.commit()
    except Exception as e:
        message = "Error updating database: {}".format(str(e))
        logger.info("Player {} - {}".format(player_id, message))
        return (False, current_pick, current_line, message)

    message = "Your pick was updated successfully! Your week {} pick is now <b>{} {}</b>".format(week, pick, f_line)
    logger.info("Player {} - {}".format(player_id, message))
    return (True, pick, line, message)



def lambda_handler(event, context):
    """
    process_pick.py

    Process LOTW picks
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
    
    # validate input
    if not validate_key(event, 'body'):
        return response(400, 'text/html', build_html_response("Bad Request [body]"))
    
    # read body of request
    input_body = event['body']
    tokens = input_body.split('&')
    params = { }
    for t in tokens:
        (key,value) = t.split('=')
        params[key] = value
    
    logger.debug("params: ".format(params))
    
    # check input parameters
    if not validate_key(params, 'pick'):
        return response(400, 'text/html', build_html_response("Bad Request [pick]"))
    else:
        pick = params['pick']
    
    if not validate_key(params, 'week'):
        return response(400, 'text/html', build_html_response("Bad Request [week]"))
    else:
        week = params['week']
        
    if not validate_key(params, 'player_id'):
        return response(400, 'text/html', build_html_response("Bad Request [player_id]"))
    else:
        player_id = params['player_id']
    
    logger.debug("validating input fields")

    # validate pick is valid team
    if not validate_field(conn, pick, "team_id", "Teams"):
        return response(400, 'text/html', build_html_response("invalid team {}".format(pick)))

    if not validate_field(conn, player_id, 'player_id', 'Players'):
        return response(400, 'text/html', build_html_response("invalid player {}".format(player_id)))
        
    if not validate_field(conn, week, 'week', "Games_" + str(get_current_year())):
        return response(400, 'text/html', build_html_response("invalid week {}".format(week)))
    
    logger.debug("calling submit_pick()")
    (res, new_pick, new_line, message) = submit_pick(conn, player_id, pick, week)

    # send email to confirm picks that were recorded successfully
    if res is True:
        logger.info("Sending email to player_id {}".format(player_id))
        # initialize variables
        mail_username = os.environ['mail_username']
        mail_password = os.environ['mail_password']
        mail_host = os.environ['mail_host']
        mail_port = os.environ['mail_port']
        mail_from = '"Brendan Connell" <bmoney312@gmail.com>'
        (player_email, player_first_name, player_last_name) = get_player_info(conn, player_id)
        logger.info("Player info: {} {} {}".format(player_first_name, player_last_name, player_email))

        mail_to = (player_email, 'bmoney312@gmail.com')
        mail_subject = "lotw: week {} pick confirmation".format(week)
        mail_body = build_html_message("Hi {},<br><br>".format(player_first_name) + message + "<br><br>Thanks, -BMC")
        email_result = send_email(mail_host, mail_port, mail_username, mail_password, mail_subject, mail_body, mail_to, mail_from)

        if email_result is True:
            logger.info("Confirmation email sent successfully")
        else:
            logger.info("Confirmation email failed")

    # close database connection
    conn.close()

    # return result
    return response(200, 'text/html', build_html_response(message))

