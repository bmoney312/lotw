import sys
import json
import logging
import datetime
from lotw import validate_field, check_auth_token
from lotw import get_kickoff_time, get_current_pick
from lotw import get_player_info, get_line, datetime_to_string, get_current_year
from lotw import build_html, build_html_head, formatted_line, response
from lotw import is_automated_scanner, get_db_connection

# global variables
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_button_html(week, player_id, pick, token):
    """
    Generate an unstyled custom interactive element that avoids standard
    <button> heuristics and verifies real human pointer coordinates.
    """
    html = """
 <form id="pickForm" method="post" action="https://w95d9hh2z8.execute-api.us-west-2.amazonaws.com/prod/picks" class="inline">
   <input type="hidden" name="week" value="{}">
   <input type="hidden" name="player_id" value="{}">
   <input type="hidden" name="pick" value="{}">
   <input type="hidden" name="token" value="{}">
   <input type="hidden" id="user_action" name="user_action" value="">
   <input type="hidden" id="interaction_proof" name="interaction_proof" value="">

   <span id="customSubmitAction" style="
       display: inline-block;
       padding: 6px 14px;
       background-color: #337ab7;
       color: #ffffff;
       border: 1px solid #2e6da4;
       border-radius: 4px;
       cursor: pointer;
       user-select: none;
       font-family: inherit;
       font-size: 14px;
       font-weight: 500;
       line-height: 1.42857143;
       text-align: center;
       vertical-align: middle;
   ">
     Confirm Pick
   </span>
 </form>
 &nbsp;
 <form action="https://w95d9hh2z8.execute-api.us-west-2.amazonaws.com/prod/submit" class="inline">
   <button type="submit" name="pick" value="XXX" class="btn btn-default">
     CANCEL
   </button>
 </form>

 <script>
   const actionEl = document.getElementById('customSubmitAction');

   actionEl.addEventListener('pointerup', function(e) {{
     // 1. Block synthetic script dispatch (isTrusted is false for programmatic events)
     if (!e.isTrusted) return;

     // 2. Reject headless runners reporting zeroed/empty coordinates
     if (e.clientX === 0 && e.clientY === 0 && e.screenX === 0) return;

     // 3. Set payload proof
     document.getElementById('user_action').value = 'human_click';
     document.getElementById('interaction_proof').value = Math.round(e.clientX) + '_' + Math.round(e.clientY);

     actionEl.innerText = 'Submitting...';
     actionEl.style.pointerEvents = 'none';

     document.getElementById('pickForm').submit();
   }});
 </script>
""".format(week, player_id, pick, token)

    return html


def lambda_handler(event, context):
    """
    submit_pick.py

    Process HTTP GET requests to display pick submission confirmation page
    """

    # Intercept warmer ping immediately
    if event.get('warmer') is True or event.get('detail-type') == 'Scheduled Warmer':
        return response(200, 'text/plain', 'warm')

    logger.info("Received event: " + json.dumps(event, indent=2))

    # check for prefetch scanners
    headers = event.get('headers', {})
    if is_automated_scanner(headers):
        logger.info("Prefetch / scanner bot detected from User-Agent: {}. Suppressing response.".format(headers.get('User-Agent', '')))
        return response(200, 'text/plain', "Prefetch detected and ignored")

    try:
        conn = get_db_connection()
    except Exception as e:
        logger.error("ERROR: Could not connect to MySQL database: {}".format(str(e)))
        sys.exit()

    logger.info("SUCCESS: Connection to MySQL database succeeded")

    # read body of request
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
            conn.close()
            return response(200, 'text/html', build_html("Pick change cancelled"))

    # validate pick is valid team
    if not validate_field(conn, pick, 'team_id', 'Teams'):
        conn.close()
        return response(400, 'text/html', build_html("invalid team {}".format(pick)))

    if not validate_field(conn, player_id, 'player_id', 'Players'):
        conn.close()
        return response(400, 'text/html', build_html("invalid player {}".format(player_id)))

    if not validate_field(conn, week, 'week', "Games_" + str(get_current_year())):
        conn.close()
        return response(400, 'text/html', build_html("invalid week {}".format(week)))

    if token is None:
        message = "invalid token"
        logger.info("Player {} - {}".format(player_id, message))
        conn.close()
        return response(400, 'text/html', build_html(message))

    # check auth token
    (authenticated, message) = check_auth_token(conn, token, player_id, week)

    if not authenticated:
        logger.info("Player {} - {}".format(player_id, message))
        conn.close()
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
        conn.close()
        return response(200, 'text/html', html)

    # confirm pick is still available
    time_now = datetime.datetime.now()
    kickoff_time = get_kickoff_time(conn, pick, week)

    if time_now >= kickoff_time:
        logger.info("{} is off the board for week {}. Kickoff time has passed {}".format(pick, week, datetime_to_string(kickoff_time)))
        message += "{} is off the board for week {}. Kickoff time has passed ({}).</h5>".format(pick, week, datetime_to_string(kickoff_time))
        html = build_html_head() + message + "</span></body></html>"
        conn.close()
        return response(200, 'text/html', html)

    # confirm game has valid line (line must be posted, not null)
    if line is None:
        logger.info("{} is off the board currently for week {}. Please select a different team.".format(pick, week))
        message += "{} is off the board currently for week {}. Please select a different team.</h5>".format(pick, week)
        html = build_html_head() + message + "</span></body></html>"
        conn.close()
        return response(200, 'text/html', html)

    if current_pick == "NOP" or current_pick is None:
        logger.info("confirm new pick {} for player {}".format(pick, player_id))
        message += "please confirm your week {} pick <b>{} {}</b></h5>".format(week, pick, formatted_line(line))
        message += get_button_html(week, player_id, pick, token)
    else:
        if pick == current_pick:
            logger.info("confirm pick {} for player {} no change from {}".format(pick, player_id, current_pick))
            message += "please confirm your week {} pick <b>{} {}</b></h5>".format(week, pick, formatted_line(line))
        else:
            logger.info("confirm pick change to {} for player {} changed from {}".format(pick, player_id, current_pick))
            message += "please confirm your pick change to <b>{} {}</b></h5>".format(pick, formatted_line(line))
        message += get_button_html(week, player_id, pick, token)

    html = build_html_head() + message + "</span></body></html>"

    # close database connection
    conn.close()

    # return result
    return response(200, 'text/html', html)
