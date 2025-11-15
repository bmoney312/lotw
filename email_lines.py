import os
import sys
import json
import pymysql
import logging
import datetime
from time import sleep
from lotw import get_current_week, get_all_paid_players, get_player, get_current_pick, get_team_name
from lotw import get_auth_token, create_auth_token, datetime_to_string, get_current_year
from lotw import build_html, formatted_line, response, send_email, smtp_connect, smtp_send

# global variables
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def build_lines_table_row(conn, player_id, week, kickoff_time, away_team_id, home_team_id, home_team_line, token):
    """
    Build lines email table row
    """

    f_kickoff_time = datetime_to_string(kickoff_time)
    time_now = datetime.datetime.now().replace(second=0, microsecond=0)
    away_team_name = get_team_name(conn, away_team_id)
    home_team_name = get_team_name(conn, home_team_id)

    if home_team_line is None or kickoff_time <= time_now:
        game_line = "OFF"

        table_row = """
    <tr>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
    </tr>
""".format(f_kickoff_time, away_team_name, home_team_name, game_line)
    else:
        if home_team_line == 0:
            game_line = "PK"
        elif home_team_line > 0:
            game_line = "{} {}".format(away_team_id, formatted_line(-home_team_line))
        else:
            game_line = "{} {}".format(home_team_id, formatted_line(home_team_line))

        table_row = """
    <tr>
        <td>{}</td>
        <td><a href="https://w95d9hh2z8.execute-api.us-west-2.amazonaws.com/prod/submit?week={}&id={}&pick={}&token={}">{}</a></td>
        <td><a href="https://w95d9hh2z8.execute-api.us-west-2.amazonaws.com/prod/submit?week={}&id={}&pick={}&token={}">{}</a></td>
        <td>{}</td>
    </tr>
""".format(f_kickoff_time, week, player_id, away_team_id, token, away_team_name, week, player_id, home_team_id, token, home_team_name, game_line)

    return table_row




def build_lines_email_head():
    """
    Build lines email head
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



def build_lines_email_body(conn, player_id, week, token):
    """
    Given database connection and current week, return body of
    LOTW line email without the html/body tags
    """

    html = "<h3>LOTW: WEEK {} LINES</h3>\n".format(week)
    # adjust header for playoff rounds
    if week == 19:
        html = "<h3>LOTW: WEEK {} LINES (WILDCARD WEEKEND)</h3>\n".format(week)
    elif week == 20:
        html = "<h3>LOTW: WEEK {} LINES (DIVISIONAL PLAYOFFS)</h3>\n".format(week)
    elif week == 21:
        html = "<h3>LOTW: WEEK {} LINES (CONFERENCE CHAMPIONSHIPS)</h3>\n".format(week)
    elif week == 22:
        html = "<h3>LOTW: SUPER BOWL LINE</h3>\n"

    html = html + """
<table>
<tr>
    <th>Kickoff Time&#42;&#42;</th>
    <th>Away Team</th>
    <th>Home Team</th>
    <th>Line</th>
</tr>
"""

    # read games for week and populate table
    with conn.cursor() as cur:
        select_statement = "SELECT `kickoff_time`, `away_team_id`, `home_team_id`, `home_team_line` FROM Games_" + str(get_current_year()) + " WHERE `week` = %s ORDER BY kickoff_time"
        logger.debug("build_lines_email_body(): {}".format(select_statement))
        cur.execute(select_statement, (week,))
        rows = cur.fetchall()
        for row in rows:
            (kickoff_time, away_team_id, home_team_id, home_team_line) = row
            logger.debug("build_lines_email_body(): processing row {} {} {} {}".format(kickoff_time, away_team_id, home_team_id, home_team_line))
            html_row = build_lines_table_row(conn, player_id, week, kickoff_time, away_team_id, home_team_id, home_team_line, token)
            html = html + html_row
    
    html = html + "</table><p>&#42;&#42;<font size=-1><b>all times US/Eastern timezone</b></font></p><br>"
    html = html + "<br><a href=\"https://aws.amazon.com/what-is-cloud-computing\"><img src=\"https://d0.awsstatic.com/logos/powered-by-aws.png\" alt=\"Powered by AWS Cloud Computing\"></a></body></html>"
    return html



def lambda_handler(event, context):
    """
    Email LOTW lines to each player
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
    mail_from = '"Brendan Connell" <bmoney312@lock-of-the-week.com>'

    # determine current week
    week = os.environ.get('week')
    player_id = os.environ.get('player_id')
    start_with_player_id = os.environ.get('start_with_player_id')

    if start_with_player_id:
        start_with_player_id = int(start_with_player_id)
        logger.info("Starting with player_id {}".format(start_with_player_id))

    if week is None:
        week = get_current_week(conn)
    else:
        week = int(week)

    if week is None:
        logger.error("ERROR: Unable to determine current week!")
        sys.exit()

    logger.info("Current week set to {}".format(week))
    logger.info("Current time is {}".format(datetime.datetime.now()))

    players = []
    if request_type == "Scheduled Event":
        players = get_all_paid_players(conn)
        logger.debug("all players: {}".format(players))
    elif request_type == "manual_run":
        if player_id is not None:
            players = get_player(conn, int(player_id))
        else:
            players = get_all_paid_players(conn)
            logger.debug("all players: {}".format(players))
    elif request_type == "test":
        players = get_player(conn, '0000000001')
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
        (player_id, player_email, last_name, first_name, titles, is_rookie) = row
        logger.info("Working on player {} {} {} {}".format(player_id, first_name, last_name, player_email))

        # skip players less than start_with_player_id
        # if start_with_player_id provided
        if start_with_player_id is not None and request_type != "test":
            if player_id < start_with_player_id:
                logger.info("Skipping player {} which is less than start_with_player_id {}".format(player_id, start_with_player_id))
                continue

        # check for existing auth token and create if none exist
        auth_token = get_auth_token(conn, player_id, week)
        if auth_token is None:
            logger.info("Creating new auth token for player_id {} week {}".format(player_id, week))
            auth_token = create_auth_token(conn, player_id, week)            
        else:
            logger.info("Found existing auth token for player_id {} week {}".format(player_id, week))

        logger.debug("Auth token for player_id {} week {} is {}".format(player_id, week, auth_token))

        # get player data
        (current_pick_id, current_pick, current_line, current_pick_ats, current_pick_lock_in) = get_current_pick(conn, player_id, week)

        # skip email if pick is already locked in
        if current_pick_lock_in:
            if request_type != "test":
                logger.info("Player {} {} pick locked in {} {}, lines not sent".format(player_id, player_email, current_pick, formatted_line(current_line)))
                continue
            else:
                logger.info("Sending email even though pick for player {} is locked in".format(player_email))

        # build message body
        message = "<body>\n<p>Hello {},<br><br>".format(first_name)
        if current_pick == "NOP" or current_pick is None:
            message = message + "You do not have a recorded week {} pick. ".format(week)
            message = message + "Please <b>click the link of a team below</b> to make your selection.<br><br>"
        else:
            printable_line = formatted_line(current_line)
            message = message + "Your week {} pick is <b>{} {}</b>. ".format(week, current_pick, printable_line)
            if current_pick_lock_in:
                message = message + "Your pick is locked in and cannot be changed.<br><br>"
            else:
                message = message + "If you would like to change your pick, please click the link of a different team below.<br><br>"

        message += "<b>DO NOT FORWARD THIS MESSAGE</b>. If you do the recipient will be able to submit picks on your behalf and view your pick for this week.<br><br>"

        # build email body for this player
        mail_body = build_lines_email_head() + message + build_lines_email_body(conn, player_id, week, auth_token)

        mail_to = (player_email, 'bmoney312@gmail.com')
        mail_subject = "lotw: week {} lines".format(week)

        # adjust subject for playoff rounds
        if week == 19:
            mail_subject = "lotw: week {} lines (wildcard weekend)".format(week)
        elif week == 20:
            mail_subject = "lotw: week {} lines (divisional playoffs)".format(week)
        elif week == 21:
            mail_subject = "lotw: week {} lines (conference championships)".format(week)
        elif week == 22:
            mail_subject = "lotw: super bowl line"

        email_result = smtp_send(smtp_relay, mail_subject, mail_body, mail_to, mail_from)

        if email_result is True:
            logger.info("Email sent successfully to player {} {}".format(player_id, player_email))
        else:
            logger.error("Email failed to player {} {}".format(player_id, player_email))
            logger.info("Sleeping for 15 seconds before retry...")
            smtp_relay.close()
            sleep(15)

            # connect to SMTP relay
            smtp_relay = None
            smtp_relay = smtp_connect(mail_host, mail_port, mail_username, mail_password)

            if smtp_relay is None:
                logger.error("Error establishing SMTP connection with {}".format(mail_host))
                conn.close()
                return response(200, 'text/html', build_html("Lines for week {} send failed for some players ({}).".format(week, player_id)))

            email_result = smtp_send(smtp_relay, mail_subject, mail_body, mail_to, mail_from)
            if email_result is True:
                logger.info("Retry email sent successfully to player {} {}".format(player_id, player_email))
            else:
                logger.error("Retry email failed to player {} {}, exiting".format(player_id, player_email))
                smtp_relay.close()
                conn.close()
                return response(504, 'text/html', build_html("Lines for week {} send failed for some players ({}).".format(week, player_id)))

    # close database connection
    conn.close()

    # close SMTP connection
    smtp_relay.close()

    # return result
    return response(200, 'text/html', build_html("Lines for week {} sent successfully.".format(week)))

