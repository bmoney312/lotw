import os
import sys
import json
import pymysql
import logging
import datetime
from time import sleep
from lotw import get_all_paid_players, get_player, get_standings, get_standings_full_name
from lotw import get_current_week, get_current_pick, get_standings_message
from lotw import build_html, formatted_line, response, build_html_head, smtp_send, smtp_connect

# global variables
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_standings_html(conn, week, standings, current_player_id):
    """
    Return string of LOTW standings in HTML table
    """
    html = "<br><br><h4>LOTW: WEEK {} STANDINGS</h4>".format(week)
    # adjust header for playoff rounds
    if week == 19:
        html = "<br><br><h4>LOTW: WEEK {} STANDINGS (WILDCARD WEEKEND)</h4>\n".format(week)
    elif week == 20:
        html = "<br><br><h4>LOTW: WEEK {} STANDINGS (DIVISIONAL PLAYOFFS)</h4>\n".format(week)
    elif week == 21:
        html = "<br><br><h4>LOTW: WEEK {} STANDINGS (CONFERENCE CHAMPIONSHIPS)</h4>\n".format(week)
    elif week == 22:
        html = "<br><br><h4>LOTW: WEEK {} STANDINGS (SUPER BOWL)</h4>\n".format(week)

    html += """
<table>
<tr>
    <th>Rank</th>
    <th>Name</th>
    <th>Wins</th>
    <th>Losses</th>
    <th>Win %</th>
    <th>ATS Points</th>
    <th>Week {} Pick</th>
    <th>Week {} Result</th>
</tr>
""".format(week, week, week)

    rank = 1
    for row in standings:
        (player_id, last_name, first_name, past_titles, rookie, wins, losses, win_percentage, ats_points) = row
        full_name = get_standings_full_name(first_name, last_name, past_titles, rookie)
        (pick_id, pick, line, pick_ats, locked_in) = get_current_pick(conn, player_id, week)

        # highlight row of current player
        highlight_row = False
        if player_id == current_player_id:
            highlight_row = True

        # set pick_ats for no picks
        if pick == "NOP" and pick_ats is None:
            pick_ats = 0
            locked_in = True
            pick_as_string = "NO PICK"
        else:
            pick_as_string = "{} {}".format(pick, formatted_line(line))

        if pick_ats > 0:
            pick_ats_as_string = "+{}".format(pick_ats)
        else:
            pick_ats_as_string = str(pick_ats)

        if pick_ats is None:
            logger.error("Unexpected NULL value for pick_ats player {} week {}".format(player_id, week))
            sys.exit()

        if pick_ats > 0:
            result = "Win (<font color=green>{}</font>)".format(pick_ats_as_string)
        else:
            result = "Loss (<font color=red>{}</font>)".format(pick_ats_as_string)

        if locked_in is not True:
            logger.error("Unexpected value locked_in value {} for player {} when creating standings HTML string".format(locked_in, player_id))
            sys.exit()

        win_percentage_string = "{0:.3f}".format(win_percentage)

        html += build_standings_html_row(rank, full_name, wins, losses, win_percentage_string, ats_points, pick_as_string, result, highlight_row)
        rank += 1 

        # end for
    html += "</table>"
    html = html + "<br><a href=\"https://aws.amazon.com/what-is-cloud-computing\"><img src=\"https://d0.awsstatic.com/logos/powered-by-aws.png\" alt=\"Powered by AWS Cloud Computing\"></a></body></html>"
    return html


def build_standings_html_row(rank, full_name, wins, losses, win_percentage, ats_points, pick_as_string, result, highlight_row):
    """
    Build HTML string of single row in standings
    """
    if highlight_row is True:
        html = """
<tr>
<td><b>{}</b></td>
<td><b>{}</b></td>
<td><b>{}</b></td>
<td><b>{}</b></td>
<td><b>{}</b></td>
<td><b>{}</b></td>
<td><b>{}</b></td>
<td><b>{}</b></td>
</tr>""".format(rank, full_name, wins, losses, win_percentage, ats_points, pick_as_string, result)
    else:
        html = """
<tr>
<td>{}</td>
<td>{}</td>
<td>{}</td>
<td>{}</td>
<td>{}</td>
<td>{}</td>
<td>{}</td>
<td>{}</td>
</tr>""".format(rank, full_name, wins, losses, win_percentage, ats_points, pick_as_string, result)

    return html



def lambda_handler(event, context):
    """
    Email LOTW standings to each player each week
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

    # take single player_id as input if provided
    player_id = os.environ.get('player_id')
    start_with_player_id = os.environ.get('start_with_player_id')

    if start_with_player_id:
        start_with_player_id = int(start_with_player_id)
        logger.info("Starting with player_id {}".format(start_with_player_id))

    if request_type == "Scheduled Event":
        players = get_all_paid_players(conn)
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
    logger.debug("Players {}".format(players))

    # week to compute standings, set to last week unless
    # environment variable week set then use same week
    standings_week = 0

    # determine current week
    week = os.environ.get('week')

    # if week is not provided
    if week is None:
        week = get_current_week(conn)
        if week is None:
            logger.error("ERROR: Unable to determine current week!")
            sys.exit()
        standings_week = int(week) - 1
    else:
        standings_week = int(week)

    logger.info("Current week set to {}".format(week))
    logger.info("Standings week set to {}".format(standings_week))
    logger.info("Current time is {}".format(datetime.datetime.now()))

    # get current standings
    standings = get_standings(conn)

    # get standings commish message
    if standings_week == 0:
        commish_message = 'Testing. Week 0 Standings.<br>'
    else:
        commish_message = get_standings_message(conn, standings_week)

    if commish_message is None:
        if request_type == "test":
            commish_message = "Testing standings for week {}.<br>".format(standings_week)
        else:
            logger.error("Unexpected missing value for commish message")
            sys.exit()

    # connect to SMTP relay
    smtp_relay = smtp_connect(mail_host, mail_port, mail_username, mail_password)

    if smtp_relay is None:
        logger.error("Error establishing SMTP connection with {}".format(mail_host))
        sys.exit()

    # email standings to each player
    for player in players:
        (player_id, player_email, last_name, first_name, titles, is_rookie) = player
        logger.info("Working on player {} {} {} {}".format(player_id, first_name, last_name, player_email))

        # skip players less than start_with_player_id
        # if start_with_player_id provided
        if start_with_player_id is not None and request_type != "test":
            if player_id < start_with_player_id:
                logger.info("Skipping player {} which is less than start_with_player_id {}".format(player_id, start_with_player_id))
                continue

        # build message body
        #message = "<body>\n<p>Hi {},<br><br>".format(first_name)
        message = "<body>\n";

        # build email body for this player
        standings_html = get_standings_html(conn, standings_week, standings, player_id)
        mail_body = build_html_head() + message + commish_message + standings_html + "<br></body></html>"
        mail_to = (player_email, 'bmoney312@gmail.com')
        mail_subject = "lotw: week {} standings".format(standings_week)

        # adjust subject for playoff rounds
        if standings_week == 19:
            mail_subject = "lotw: week {} standings (wildcard weekend)".format(standings_week)
        elif standings_week == 20:
            mail_subject = "lotw: week {} standings (divisional playoffs)".format(standings_week)
        elif standings_week == 21:
            mail_subject = "lotw: week {} standings (conference championships)".format(standings_week)
        elif standings_week == 22:
            mail_subject = "lotw: week {} standings (super bowl)".format(standings_week)

        email_result = smtp_send(smtp_relay, mail_subject, mail_body, mail_to, mail_from)

        if email_result is True:
            logger.info("Email sent successfully to {} {}".format(player_id, player_email))
        else:
            logger.debug("Email to {} {} failed".format(player_id, player_email))
            logger.info("Sleeping for 15 seconds before retry...")
            smtp_relay.close()
            sleep(15)

            # connect to SMTP relay
            smtp_relay = None
            smtp_relay = smtp_connect(mail_host, mail_port, mail_username, mail_password)

            if smtp_relay is None:
                logger.error("Error establishing SMTP connection with {}".format(mail_host))
                conn.close()
                return response(200, 'text/html', build_html("Standings for week {} send failed for some players ({}).".format(week, player_id)))

            email_result = smtp_send(smtp_relay, mail_subject, mail_body, mail_to, mail_from)
            if email_result is True:
                logger.info("Retry email sent successfully to player {} {}".format(player_id, player_email))
            else:
                logger.error("Retry email failed to player {} {}, exiting".format(player_id, player_email))
                smtp_relay.close()
                conn.close()
                return response(504, 'text/html', build_html("Standings for week {} send failed for some players ({}).".format(week, player_id)))

    # close database connection
    conn.close()

    # close SMTP connection
    smtp_relay.close()

    # return result
    return response(200, 'text/html', build_html("Standings for week {} sent successfully.".format(standings_week)))

