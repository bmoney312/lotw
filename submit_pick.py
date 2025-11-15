import os
import sys
import json
import pymysql
import logging
import datetime
from lotw import validate_field, check_auth_token
from lotw import get_kickoff_time, get_current_pick
from lotw import get_player_info, get_line, datetime_to_string, get_current_year
from lotw import build_html, build_html_head, formatted_line, response

# global variables
logger = logging.getLogger()
logger.setLevel(logging.INFO)



def get_button_html(week, player_id, pick):
    """
    print <button> and <form> html for submit and cancel buttons
    """
    html = """
 <form method="post" action="https://w95d9hh2z8.execute-api.us-west-2.amazonaws.com/prod/picks" class="inline">
   <input type="hidden" name="week" value="{}">
   <input type="hidden" name="player_id" value="{}">
   <button type="submit" name="pick" value="{}" class="btn btn-primary">
 SUBMIT PICK
   </button>
 </form>
 &nbsp;
  <form action="https://w95d9hh2z8.execute-api.us-west-2.amazonaws.com/prod/submit" class="inline">
   <button type="submit" name="pick" value="XXX" class="btn btn-default">
 CANCEL
   </button>
 </form>
""".format(week, player_id, pick)

    return html



def lambda_handler(event, context):
    """
    submit_pick.py

    Process HTTP GET requests to display pick submission confirmation page
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
    
    # read body of request
    #params = event;
    #query_string = json.loads(event['queryStringParameters'])
    query_string_params = event.get('queryStringParameters')

    if query_string_params is not None:
        # HTTP GET from API Gateway
        week = query_string_params.get('week')
        player_id = query_string_params.get('id')
        pick = query_string_params.get('pick')
        token = query_string_params.get('token')
    else:
        # Lambda test event
        week = event.get('week')
        player_id = event.get('id')
        pick = event.get('pick')
        token = event.get('token')

    logger.debug("Validating input fields")

    if pick is not None:
        if pick == "XXX":
            return response(200, 'text/html', build_html("Pick change cancelled"))

    # validate pick is valid team
    if not validate_field(conn, pick, 'team_id', 'Teams'):
        return response(400, 'text/html', build_html("invalid team {}".format(pick)))

    if not validate_field(conn, player_id, 'player_id', 'Players'):
        return response(400, 'text/html', build_html("invalid player {}".format(player_id)))

    if not validate_field(conn, week, 'week', "Games_" + str(get_current_year())):
        return response(400, 'text/html', build_html("invalid week {}".format(week)))

    if token is None:
        message = "invalid token"
        logger.info("Player {} - {}".format(player_id, message))
        return response(400, 'text/html', build_html(message))

    # check auth token
    (authenticated, message) = check_auth_token(conn, token, player_id, week)

    if not authenticated:
        logger.info("Player {} - {}".format(player_id, message))
        return response(400, 'text/html', build_html(message))

    # get player data
    (player_email, first_name, last_name) = get_player_info(conn, player_id)
    (current_pick_id, current_pick, current_line, current_pick_ats, current_pick_lock_in) = get_current_pick(conn, player_id, week)
    line = get_line(conn, pick, week)

    # build html document
    message = "<body><span class=\"message\">\n<h5>Hi {}, ".format(first_name)

    # first check if player's pick is locked in
    if current_pick_lock_in:
        logger.info("pick change to {} denied for player {}, current pick {} is locked in".format(pick, player_id, current_pick))
        message += "your week {} pick is locked in and cannot be changed.</h5>".format(week)
        html = build_html_head() + message + "</span></body></html>"
        return response(200, 'text/html', html)

    # confirm pick is still avaiable
    time_now = datetime.datetime.now()
    kickoff_time = get_kickoff_time(conn, pick, week)

    if time_now >= kickoff_time:
        logger.info("{} is off the board for week {}. Kickoff time has passed {}".format(pick, week, datetime_to_string(kickoff_time)))
        message += "{} is off the board for week {}. Kickoff time has passed ({}).</h5>".format(pick, week, datetime_to_string(kickoff_time))
        html = build_html_head() + message + "</span></body></html>"
        return response(200, 'text/html', html)

    # confirm game has valid line (line must be posted, not null)
    if line is None:
        logger.info("{} is off the board currently for week {}. Please select a different team.".format(pick, week))
        message += "{} is off the board currently for week {}. Please select a different team.</h5>".format(pick, week)
        html = build_html_head() + message + "</span></body></html>"
        return response(200, 'text/html', html)

    if current_pick == "NOP" or current_pick is None:
        logger.info("confirm new pick {} for player {}".format(pick, player_id))
        message += "please confirm your week {} pick <b>{} {}</b></h5>".format(week, pick, formatted_line(line))
        message += get_button_html(week, player_id, pick)
    else:
        printable_line = formatted_line(current_line)
        #message += "your week {} pick is <b>{} {}</b>. ".format(week, current_pick, printable_line)
        if pick == current_pick:
            logger.info("confirm pick {} for player {} no change from {}".format(pick, player_id, current_pick))
            message += "please confirm your week {} pick <b>{} {}</b></h5>".format(week, pick, formatted_line(line)) 
        else:
            logger.info("confirm pick change to {} for player {} changed from {}".format(pick, player_id, current_pick))
            message += "please confirm your pick change to <b>{} {}</b></h5>".format(pick, formatted_line(line))
        message += get_button_html(week, player_id, pick)

    html = build_html_head() + message + "</span></body></html>"

    # close database connection
    conn.close()

    # return result
    return response(200, 'text/html', html)

