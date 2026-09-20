import os
import sys
import json
import logging
import datetime
import boto3
from time import sleep
from lotw import get_current_week, get_all_paid_players, get_player, get_current_pick
from lotw import create_auth_token, datetime_to_string, get_current_year
from lotw import build_html, formatted_line, response, smtp_connect, smtp_send
from lotw import get_db_connection

# global variables
logger = logging.getLogger()
logger.setLevel(logging.INFO)
cloudwatch = boto3.client('cloudwatch')


def build_lines_table_row(player_id, week, kickoff_time, away_team_id, home_team_id, home_team_line, token, team_names_map):
    """
    Build lines email table row using in-memory team names map.
    """
    f_kickoff_time = datetime_to_string(kickoff_time)
    time_now = datetime.datetime.now().replace(second=0, microsecond=0)
    away_team_name = team_names_map.get(away_team_id, away_team_id)
    home_team_name = team_names_map.get(home_team_id, home_team_id)

    if home_team_line is None or kickoff_time <= time_now:
        game_line = "OFF"
        table_row = """
    <tr>
        <td style="border: 1px solid black; padding: 6px; text-align: left;">{}</td>
        <td style="border: 1px solid black; padding: 6px; text-align: left;">{}</td>
        <td style="border: 1px solid black; padding: 6px; text-align: left;">{}</td>
        <td style="border: 1px solid black; padding: 6px; text-align: left;">{}</td>
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
        <td style="border: 1px solid black; padding: 6px; text-align: left;">{}</td>
        <td style="border: 1px solid black; padding: 6px; text-align: left;"><a href="https://w95d9hh2z8.execute-api.us-west-2.amazonaws.com/prod/submit?week={}&id={}&pick={}&token={}">{}</a></td>
        <td style="border: 1px solid black; padding: 6px; text-align: left;"><a href="https://w95d9hh2z8.execute-api.us-west-2.amazonaws.com/prod/submit?week={}&id={}&pick={}&token={}">{}</a></td>
        <td style="border: 1px solid black; padding: 6px; text-align: left;">{}</td>
    </tr>
""".format(f_kickoff_time, week, player_id, away_team_id, token, away_team_name, week, player_id, home_team_id, token, home_team_name, game_line)

    return table_row


def build_lines_email_head():
    html = """
<html>
<head>
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css">
    <style>
         body {
             margin: 0;
             padding: 0;
             width: 100% !important;
             background-color: #f6f6f6;
             font-family: "Arial", "Helvetica", sans-serif;
         }
         h3 {
             text-align: left;
             margin-top: 10px;
             margin-bottom: 20px;
         }
         table.lines-table {
             width: 100%;
             max-width: 560px;
             margin: 0 auto;
             border-collapse: collapse;
             border: 1px solid black;
         }
         table.lines-table th {
             border: 1px solid black;
             padding: 6px;
             text-align: left;
             background-color: lightgrey;
         }
         table.lines-table td {
             border: 1px solid black;
             padding: 6px;
             text-align: left;
         }
         .footnote {
             text-align: center;
             margin-top: 10px;
         }
         .footer-logo {
             text-align: center;
             margin-top: 25px;
             margin-bottom: 15px;
         }
    </style>
</head>
"""
    return html


def build_lines_email_body(player_id, week, token, games_list, team_names_map):
    """
    Given pre-fetched games and team map, return body of LOTW line email without DB roundtrips.
    """
    header_title = "LOTW: WEEK {} LINES".format(week)
    if week == 19:
        header_title = "LOTW: WEEK {} LINES (WILDCARD WEEKEND)".format(week)
    elif week == 20:
        header_title = "LOTW: WEEK {} LINES (DIVISIONAL PLAYOFFS)".format(week)
    elif week == 21:
        header_title = "LOTW: WEEK {} LINES (CONFERENCE CHAMPIONSHIPS)".format(week)
    elif week == 22:
        header_title = "LOTW: SUPER BOWL LINE"

    html = '<h3 style="text-align: left;">{}</h3>\n'.format(header_title)

    html += """
<table class="lines-table" role="presentation" border="1" cellpadding="6" cellspacing="0" align="center" style="margin: 0 auto; border-collapse: collapse; width: 100%;">
<tr>
    <th style="background-color: lightgrey; border: 1px solid black; padding: 6px; text-align: left;">Kickoff Time&#42;&#42;</th>
    <th style="background-color: lightgrey; border: 1px solid black; padding: 6px; text-align: left;">Away Team</th>
    <th style="background-color: lightgrey; border: 1px solid black; padding: 6px; text-align: left;">Home Team</th>
    <th style="background-color: lightgrey; border: 1px solid black; padding: 6px; text-align: left;">Line</th>
</tr>
"""
    for row in games_list:
        kickoff_time, away_team_id, home_team_id, home_team_line = row
        html += build_lines_table_row(player_id, week, kickoff_time, away_team_id, home_team_id, home_team_line, token, team_names_map)

    html += """</table>
<div class="footnote" style="text-align: center; margin-top: 10px;">
  <p>&#42;&#42;<font size="-1"><b>all times US/Eastern timezone</b></font></p>
</div>
          </td>
        </tr>
      </table>
      <!-- End Centered Container Card -->
    </td>
  </tr>
</table>
</body></html>"""
    return html


def emit_emails_sent_metric(week, emails_sent_count, request_type=None):
    retval = False
    metric_data = [
        {
            'MetricName': 'LinesEmailsSent',
            'Dimensions': [
                {'Name': 'Year', 'Value': str(get_current_year())},
                {'Name': 'Week', 'Value': str(week)}
            ],
            'Value': emails_sent_count,
            'Unit': 'Count'
        }
    ]

    # Also emit ScheduledLinesEmailsSent when triggered by an EventBridge scheduled event
    if request_type == "Scheduled Event":
        metric_data.append({
            'MetricName': 'ScheduledLinesEmailsSent',
            'Dimensions': [
                {'Name': 'Year', 'Value': str(get_current_year())},
                {'Name': 'Week', 'Value': str(week)}
            ],
            'Value': emails_sent_count,
            'Unit': 'Count'
        })

    try:
        cloudwatch.put_metric_data(
            Namespace='lotw',
            MetricData=metric_data
        )
        logger.info(
            "Emitted metric(s) for count %s (scheduled=%s)",
            emails_sent_count,
            request_type == "Scheduled Event"
        )
        retval = True
    except Exception as e:
        logger.error("Failed to emit CloudWatch metric: %s", str(e))
    return retval


def lambda_handler(event, context):
    logger.info("Received event: " + json.dumps(event, indent=2))

    request_type = event.get('detail-type')
    if request_type is None:
        logger.error("Unable to determine request type")
        sys.exit()

    try:
        conn = get_db_connection()
    except Exception as e:
        logger.error("ERROR: Could not connect to MySQL database: {}".format(str(e)))
        sys.exit()

    logger.info("SUCCESS: Connection to MySQL database succeeded")

    mail_username = os.environ['mail_username']
    mail_password = os.environ['mail_password']
    mail_host = os.environ['mail_host']
    mail_port = os.environ['mail_port']
    mail_from = '"Brendan Connell" <bmoney312@lock-of-the-week.com>'

    try:
        MAX_RETRIES = int(os.environ.get('SMTP_RETRIES', 5))
    except ValueError:
        MAX_RETRIES = 5

    try:
        RETRY_SLEEP_SECONDS = int(os.environ.get('SMTP_RETRY_SLEEP', 15))
    except ValueError:
        RETRY_SLEEP_SECONDS = 15

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
    elif request_type == "manual_run":
        if player_id is not None:
            players = get_player(conn, int(player_id))
        else:
            players = get_all_paid_players(conn)
    elif request_type == "test":
        players = get_player(conn, '0000000001')
    else:
        logger.error("Invalid request type {}".format(request_type))
        raise

    logger.info("Request type is {}".format(request_type))

    # --- Pre-fetch Batch Data (Caches Team Names, Games, and Existing Auth Tokens) ---
    team_names_map = {}
    games_list = []
    auth_tokens_map = {}
    current_year = get_current_year()

    with conn.cursor() as cur:
        # 1. Fetch all team names in one call
        cur.execute("SELECT team_id, CONCAT(city, ' ', nickname) FROM Teams")
        for tid, tname in cur.fetchall():
            team_names_map[tid] = tname

        # 2. Fetch games for current week once
        cur.execute(
            "SELECT kickoff_time, away_team_id, home_team_id, home_team_line FROM Games_{} WHERE week = %s ORDER BY kickoff_time".format(current_year),
            (week,)
        )
        games_list = cur.fetchall()

        # 3. Fetch existing auth tokens for the week once
        cur.execute("SELECT player_id, token FROM Auth_Tokens WHERE week = %s", (week,))
        for pid, tok in cur.fetchall():
            auth_tokens_map[pid] = tok

    emails_sent_count = 0
    smtp_relay = smtp_connect(mail_host, mail_port, mail_username, mail_password)
    if smtp_relay is None:
        logger.error("Error establishing SMTP connection with {}".format(mail_host))
        sys.exit()

    for row in players:
        (player_id, player_email, last_name, first_name, titles, is_rookie) = row

        if start_with_player_id is not None and request_type != "test":
            if player_id < start_with_player_id:
                continue

        # Use in-memory token lookup or generate if missing
        auth_token = auth_tokens_map.get(player_id)
        if auth_token is None:
            logger.info("Creating new auth token for player_id {} week {}".format(player_id, week))
            auth_token = create_auth_token(conn, player_id, week)
            auth_tokens_map[player_id] = auth_token
        else:
            logger.info("Found existing auth token for player_id {} week {}".format(player_id, week))

        (current_pick_id, current_pick, current_line, current_pick_ats, current_pick_lock_in) = get_current_pick(conn, player_id, week)

        if current_pick_lock_in:
            if request_type != "test":
                logger.info("Player {} {} pick locked in {} {}, lines not sent".format(player_id, player_email, current_pick, formatted_line(current_line)))
                continue

        # Centered Outer Table & Card Container opening
        message = """<body>
<table role="presentation" border="0" cellpadding="0" cellspacing="0" width="100%" style="background-color: #f6f6f6;">
  <tr>
    <td align="center" style="padding: 20px 10px;">
      <!-- Centered Container Card -->
      <table role="presentation" border="0" cellpadding="0" cellspacing="0" width="100%" style="max-width: 600px; background-color: #ffffff; border-radius: 6px; padding: 20px;">
        <tr>
          <td>
            <p>Hello {},<br><br>""".format(first_name)

        if current_pick == "NOP" or current_pick is None:
            message += "You do not have a recorded week {} pick. ".format(week)
            message += "Please <b>click the link of a team below</b> to make your selection.<br><br>"
        else:
            printable_line = formatted_line(current_line)
            message += "Your week {} pick is <b>{} {}</b>. ".format(week, current_pick, printable_line)
            if current_pick_lock_in:
                message += "Your pick is locked in and cannot be changed.<br><br>"
            else:
                message += "If you would like to change your pick, please click the link of a different team below.<br><br>"

        message += "<b>DO NOT FORWARD THIS MESSAGE</b>. If you do the recipient will be able to submit picks on your behalf and view your pick for this week.<br><br>"
        mail_body = build_lines_email_head() + message + build_lines_email_body(player_id, week, auth_token, games_list, team_names_map)

        mail_to = (player_email, 'bmoney312@gmail.com')
        mail_subject = "lotw: week {} lines".format(week)
        if week == 19:
            mail_subject = "lotw: week {} lines (wildcard weekend)".format(week)
        elif week == 20:
            mail_subject = "lotw: week {} lines (divisional playoffs)".format(week)
        elif week == 21:
            mail_subject = "lotw: week {} lines (conference championships)".format(week)
        elif week == 22:
            mail_subject = "lotw: super bowl line"

        email_sent_successfully = False
        for attempt in range(MAX_RETRIES):
            email_result = smtp_send(smtp_relay, mail_subject, mail_body, mail_to, mail_from)
            if email_result is True:
                email_sent_successfully = True
                emails_sent_count += 1
                break
            else:
                if attempt <= MAX_RETRIES:
                    smtp_relay.close()
                    sleep(RETRY_SLEEP_SECONDS)
                    smtp_relay = smtp_connect(mail_host, mail_port, mail_username, mail_password)
                    if smtp_relay is None:
                        break

        if not email_sent_successfully:
            if smtp_relay is not None:
                smtp_relay.close()
            emit_emails_sent_metric(week, emails_sent_count, request_type)
            conn.close()
            raise RuntimeError("Lines for week {} send failed for player {} after {} attempts. Aborting.".format(week, player_id, MAX_RETRIES))
        else:
            logger.info("Lines for week {} sent successfully to player {} {}.".format(week, player_id, player_email))

        # gentle pacing
        sleep(2)

    emit_emails_sent_metric(week, emails_sent_count, request_type)
    conn.close()
    smtp_relay.close()
    logger.info("Lines for week {} sent successfully to {} players.".format(week, emails_sent_count))
    return response(200, 'text/html', build_html("Lines for week {} sent successfully to {} players.".format(week, emails_sent_count)))
