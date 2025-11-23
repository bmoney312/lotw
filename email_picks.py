import os
import sys
import json
import pymysql
import logging
import datetime
import pytz
from time import sleep
from lotw import get_current_week, get_all_paid_players, get_player, get_line, get_current_year, in_daylight_savings
from lotw import get_all_picks, get_current_pick, get_standings, get_standings_full_name
from lotw import build_html, formatted_line, response, send_email, smtp_connect, smtp_send

# global variables
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def build_picks_html_row(rank, full_name, wins, losses, ats_points, pick, highlight_row):
    """
    Build picks email table row
    """
    if highlight_row is True:
        table_row = """
<tr>
    <td><b>{}</b></td>
    <td><b>{}</b></td>
    <td><b>{}</b></td>
    <td><b>{}</b></td>
    <td><b>{}</b></td>
    <td><b>{}</b></td>
</tr>
""".format(rank, full_name, wins, losses, ats_points, pick)
    else:
        table_row = """
<tr>
    <td>{}</td>
    <td>{}</td>
    <td>{}</td>
    <td>{}</td>
    <td>{}</td>
    <td>{}</td>
</tr>
""".format(rank, full_name, wins, losses, ats_points, pick)

    return table_row



def build_picks_email_head():
    """
    Build picks email head
    """

    html = """
<html>
<head>
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
             padding: 5px;
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



def build_picks_email_body(week, standings, current_picks, message, send_pick_summary, current_player_id):
    """
    Given database connection and current week, return body of
    LOTW line email without the html/body tags

    send_pick_summary is boolean, send pick for all players if true (even NOP)

    current_picks is dict of format player_id => teams
    """
    html = "<body>\n<p>{}</p><br>".format(message)

    # adjust header for playoff rounds
    if week == 19:
        html += "<h4>LOTW: WEEK {} PICKS (WILDCARD WEEKEND)</h4>\n".format(week)
    elif week == 20:
        html += "<h4>LOTW: WEEK {} PICKS (DIVISIONAL PLAYOFFS)</h4>\n".format(week)
    elif week == 21:
        html += "<h4>LOTW: WEEK {} PICKS (CONFERENCE CHAMPIONSHIPS)</h4>\n".format(week)
    elif week == 22:
        html += "<h4>LOTW: WEEK {} PICKS (SUPER BOWL)</h4>\n".format(week)
    else:
        html += "<h4>LOTW: WEEK {} PICKS</h4>\n".format(week)

    #html += "<h4>LOTW: WEEK {} PICKS</h4>\n".format(week)
    html += """
<table>
<tr>
    <th>Rank</th>
    <th>Player</th>
    <th>Wins</th>
    <th>Losses</th>
    <th>ATS Points</th>
    <th>Week {} Pick</th>
</tr>
""".format(week, week)

    rank = 1
    for row in standings:
        (player_id, last_name, first_name, past_titles, rookie, wins, losses, win_percentage, ats_points) = row
        full_name = get_standings_full_name(first_name, last_name, past_titles, rookie)

        # highlight player's own pick
        highlight_row = False
        if player_id == current_player_id:
            highlight_row = True

        pick_as_string = ""
        if current_picks.get(player_id) is None and send_pick_summary is not True:
            logger.debug("Player {} not found in current picks".format(player_id))
            rank += 1
            continue
        else:
            if current_picks.get(player_id) is None:
                pick_as_string = "<i>&lt;hidden&gt;</i>"
            else:
                (pick, line) = current_picks.get(player_id)
                pick_as_string = "{} {}".format(pick, formatted_line(line))

            logger.debug("Found pick {} for player {} in current picks".format(pick_as_string, player_id))

        # no ranks yet in week 1
        if week == 1:
            rank_as_string = "-"
        else:
            rank_as_string = "{}".format(rank)

        html += build_picks_html_row(rank_as_string, full_name, wins, losses, ats_points, pick_as_string, highlight_row)
        rank += 1

    # end for
    
    html = html + "</table>"
    html = html + "<br><br><a href=\"https://aws.amazon.com/what-is-cloud-computing\"><img src=\"https://d0.awsstatic.com/logos/powered-by-aws.png\" alt=\"Powered by AWS Cloud Computing\"></a></body></html>"
    return html


def get_picks_at_kickoff_time(conn, week, lock_in_time, send_pick_summary):
    """
    return list of tuples containing picks that lock in at lock_in_time
    if send_pick_summary is True, fetch all picks

    return None if no picks match
    """
    with conn.cursor() as cur:
        if send_pick_summary is True:
            select_statement = "SELECT `player_id`, `pick` FROM Picks_" + str(get_current_year()) + " WHERE `week` = %s AND `lock_in_time` IS NOT NULL AND `lock_in_time` < CURRENT_TIMESTAMP"
            cur.execute(select_statement, (week, ))
        else:
            select_statement = "SELECT `player_id`, `pick` FROM Picks_" + str(get_current_year()) + " WHERE `week` = %s AND `lock_in_time` = %s"
            cur.execute(select_statement, (week, lock_in_time))
        logger.debug("get_picks_at_kickoff_time(): SQL {}".format(select_statement))
        rows = cur.fetchall()
        picks = {}
        for row in rows:
            (player_id, pick) = row
            if pick is not None:
                line = get_line(conn, pick, week)
            picks[player_id] = (pick, line)

        logger.debug("get_picks_at_kickoff_time(): Found these picks at kickoff time {}: {}".format(lock_in_time, picks))
        return picks



def lambda_handler(event, context):
    """
    Email LOTW picks to each player at start of each game
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

    # take single player_id as input if provided
    player_id = os.environ.get('player_id')
    start_with_player_id = os.environ.get('start_with_player_id')

    if start_with_player_id:
        start_with_player_id = int(start_with_player_id)
        logger.info("Starting with player_id {}".format(start_with_player_id))

    # determine current week
    week = get_current_week(conn)

    if week is None:
        logger.error("ERROR: Unable to determine current week!")
        sys.exit()
    else:
        week = int(week)

    send_pick_summary = os.environ.get('send_pick_summary')
    if send_pick_summary is not None:
        if send_pick_summary == 'True':
            send_pick_summary = True
        else:
            send_pick_summary = False
    else:
        send_pick_summary = False

    logger.debug("send_pick_summary is {}".format(send_pick_summary))

    # current time, set second and microsecond to 0 to match kickoff times
    time_now = datetime.datetime.now().replace(second=0, microsecond=0)
    #time_now = datetime.datetime.now().replace(minute=15, second=0, microsecond=0)

    # set pick deadline to current time
    # scheduled events should align with game times to the minute
    # pick summaries should be sent upon manual runs or no games will be returned
    pick_deadline = time_now

    logger.info("Current week set to {}".format(week))
    logger.info("Current time is {}".format(time_now))
    logger.info("Pick deadline is {}".format(pick_deadline))

    players = []
    if request_type == "Scheduled Event":
        players = get_all_paid_players(conn)
        event_type = event.get('resources')
        email_pick_summary_arn = "arn:aws:events:us-west-2:062043405251:rule/Email_Pick_Summary"
        if event_type is not None:
            if event_type[0] == email_pick_summary_arn:
                send_pick_summary = True
    elif request_type == "manual_run":
        if player_id is not None:
            players = get_player(conn, int(player_id))
        else:
            players = get_all_paid_players(conn)
    elif request_type == "test":
        players = get_player(conn, int(1))
    else:
        logger.error("Invalid request type {}".format(request_type))
        sys.exit()

    logger.info("Request type is {}".format(request_type))
    logger.info("Players {}".format(players))

    if send_pick_summary is True:
        commish_message = "All locked in week {} picks given below.  Good luck! -BMC<br>".format(week)
        logger.info("Sending full pick summary")
    else:
        commish_message = "The picks below are now <b>locked in</b>. Good luck! -BMC<br>"
        weekday = time_now.weekday()
        if weekday == 4:
            commish_message = "The following players like the Thursday night special tonight. " + commish_message

    # get standings and current picks
    standings = get_standings(conn)
    player_picks = get_picks_at_kickoff_time(conn, week, pick_deadline, send_pick_summary)
    logger.debug("player_picks: {}".format(player_picks))

    if len(player_picks) == 0:
        return response(404, 'text/html', build_html("No picks found that locked in at {}".format(pick_deadline)))

    # connect to SMTP relay
    smtp_relay = smtp_connect(mail_host, mail_port, mail_username, mail_password)

    if smtp_relay is None:
        logger.error("Error establishing SMTP connection with {}".format(mail_host))
        sys.exit()

    # send email to each player
    for row in players:
        (player_id, player_email, last_name, first_name, titles, is_rookie) = row
        logger.info("Working on player {} {} {} {}".format(player_id, first_name, last_name, player_email))

        # skip players less than start_with_player_id
        # if start_with_player_id provided
        if start_with_player_id is not None and request_type != "test":
            if player_id < start_with_player_id:
                logger.info("Skipping player {} which is less than start_with_player_id {}".format(player_id, start_with_player_id))
                continue

        # build email body for this player
        picks_email_body = build_picks_email_body(week, standings, player_picks, commish_message, send_pick_summary, player_id)
        mail_body = build_picks_email_head() + picks_email_body

        mail_to = (player_email, 'bmoney312@lock-of-the-week.com')
        mail_subject = "lotw: week {} picks".format(week)

        # adjust subject for playoff rounds
        if week == 19:
            mail_subject = "lotw: week {} picks (wildcard weekend)".format(week)
        elif week == 20:
            mail_subject = "lotw: week {} picks (divisional playoffs)".format(week)
        elif week == 21:
            mail_subject = "lotw: week {} picks (conference championships)".format(week)
        elif week == 22:
            mail_subject = "lotw: week {} picks (super bowl)".format(week)

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
            return response(504, 'text/html', build_html("Picks for week {} send failed for player {} after {} attempts. Aborting.".format(week, player_id, MAX_RETRIES)))

    # close database connection
    conn.close()

    # close SMTP relay connection
    smtp_relay.close()

    # return result
    return response(200, 'text/html', build_html("Picks for week {} sent successfully.".format(week)))

