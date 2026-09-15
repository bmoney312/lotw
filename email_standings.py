import os
import sys
import json
import logging
import datetime
import boto3
from time import sleep
from lotw import get_all_paid_players, get_player, get_standings_full_name
from lotw import get_current_week, get_current_year, get_db_connection
from lotw import build_html, formatted_line, response, build_html_head, smtp_send, smtp_connect

# global variables
logger = logging.getLogger()
logger.setLevel(logging.INFO)
cloudwatch = boto3.client('cloudwatch')


def get_standings_by_year(conn, year):
    """
    Return LOTW standings with player names and attributes for the given year.
    """
    with conn.cursor() as cur:
        select_statement = (
            f"SELECT Standings_{year}.player_id, `last_name`, `first_name`, "
            "`past_titles`, `rookie`, `wins`, `losses`, `win_percentage`, `ats_points`, `streak` "
            f"FROM Standings_{year} INNER JOIN Players ON Standings_{year}.player_id = Players.player_id "
            "ORDER BY win_percentage DESC, ats_points DESC, last_name ASC, first_name ASC"
        )
        logger.debug("get_standings_by_year(): %s", select_statement)
        cur.execute(select_statement)
        return cur.fetchall()


def get_standings_message_by_year(conn, week, year):
    """
    Get weekly commissioner standings message string for the given year.
    """
    with conn.cursor() as cur:
        select_statement = f"SELECT `message` FROM `Standings_Message_{year}` WHERE `week` = %s"
        cur.execute(select_statement, (week, ))
        result = cur.fetchone()
        return result[0] if result is not None else None


def get_standings_html(week, standings, current_player_id, picks_map):
    """
    Return string of LOTW standings in HTML table
    """
    html = f"<br><br><h3>LOTW: WEEK {week} STANDINGS</h3>"
    if week == 19:
        html = f"<br><br><h3>LOTW: WEEK {week} STANDINGS (WILDCARD WEEKEND)</h3>\n"
    elif week == 20:
        html = f"<br><br><h3>LOTW: WEEK {week} STANDINGS (DIVISIONAL PLAYOFFS)</h3>\n"
    elif week == 21:
        html = f"<br><br><h3>LOTW: WEEK {week} STANDINGS (CONFERENCE CHAMPIONSHIPS)</h3>\n"
    elif week == 22:
        html = f"<br><br><h3>LOTW: WEEK {week} STANDINGS (SUPER BOWL)</h3>\n"

    html += f"""
<table>
<tr>
    <th>Rank</th>
    <th>Name</th>
    <th>Wins</th>
    <th>Losses</th>
    <th>Win %</th>
    <th>ATS Points</th>
    <th>Streak</th>
    <th>Week {week} Pick</th>
    <th>Week {week} Result</th>
</tr>
"""

    rank = 1
    for row in standings:
        (player_id, last_name, first_name, past_titles, rookie, wins, losses, win_percentage, ats_points, streak) = row
        full_name = get_standings_full_name(first_name, last_name, past_titles, rookie)
        pick_data = picks_map.get(player_id, ("NOP", None, None, False))
        (pick, line, pick_ats, locked_in) = pick_data

        highlight_row = (player_id == current_player_id)

        if pick == "NOP" and pick_ats is None:
            pick_ats = 0
            locked_in = True
            pick_as_string = "NO PICK"
        else:
            pick_as_string = f"{pick} {formatted_line(line)}"

        if pick_ats is not None:
            pick_ats_as_string = f"+{pick_ats}" if pick_ats > 0 else str(pick_ats)
        else:
            logger.error("Unexpected NULL value for pick_ats player %s week %s", player_id, week)
            sys.exit()

        if pick_ats > 0:
            result = f"Win (<font color=green>{pick_ats_as_string}</font>)"
        else:
            result = f"Loss (<font color=red>{pick_ats_as_string}</font>)"

        if locked_in is not True:
            logger.error("Unexpected locked_in value %s for player %s when creating standings HTML string", locked_in, player_id)
            sys.exit()

        win_percentage_string = f"{win_percentage:.3f}"
        html += build_standings_html_row(rank, full_name, wins, losses, win_percentage_string, ats_points, streak, pick_as_string, result, highlight_row)
        rank += 1

    html += "</table>"
    html += "<br><a href=\"https://aws.amazon.com/what-is-cloud-computing\"><img src=\"https://d0.awsstatic.com/logos/powered-by-aws.png\" alt=\"Powered by AWS Cloud Computing\"></a></body></html>"
    return html


def build_standings_html_row(rank, full_name, wins, losses, win_percentage, ats_points, streak, pick_as_string, result, highlight_row):
    """
    Build HTML string of single row in standings
    """
    if highlight_row:
        return f"""
<tr>
<td><b>{rank}</b></td>
<td><b>{full_name}</b></td>
<td><b>{wins}</b></td>
<td><b>{losses}</b></td>
<td><b>{win_percentage}</b></td>
<td><b>{ats_points}</b></td>
<td><b>{streak}</b></td>
<td><b>{pick_as_string}</b></td>
<td><b>{result}</b></td>
</tr>"""
    else:
        return f"""
<tr>
<td>{rank}</td>
<td>{full_name}</td>
<td>{wins}</td>
<td>{losses}</td>
<td>{win_percentage}</td>
<td>{ats_points}</td>
<td>{streak}</td>
<td>{pick_as_string}</td>
<td>{result}</td>
</tr>"""


def get_player_season_details_cached(player_id, player_picks_by_player, games_by_week_team):
    """
    Get weekly breakdown: Week, Pick, Game Result, Site, Result, Fav/Dog status.
    """
    rows = player_picks_by_player.get(player_id, [])
    weekly_data = []
    fav_count = 0
    dog_count = 0
    pickem_count = 0

    for row in rows:
        week, pick, pick_ats = row
        game = games_by_week_team.get((week, pick))

        classification, line, site, game_result = "-", None, "-", "-"
        if game:
            home_team_id, away_team_id, home_line, away_score, home_score = game
            if home_line is not None:
                if pick == home_team_id:
                    relevant_line = home_line
                    site = "Home"
                else:
                    relevant_line = -home_line
                    site = "Road"

                line = relevant_line
                if relevant_line < 0:
                    classification = "Favorite"
                    fav_count += 1
                elif relevant_line > 0:
                    classification = "Underdog"
                    dog_count += 1
                else:
                    classification = "Pick'em"
                    pickem_count += 1

            if away_score is not None and home_score is not None:
                game_result = f"{away_team_id} {away_score} v {home_team_id} {home_score}"

        pick_display = f"{pick} {formatted_line(line)}" if line is not None else pick
        if pick_ats is not None:
            result_str = "Win" if pick_ats > 0 else ("Loss" if pick_ats < 0 else "Loss (Push)")
        else:
            result_str = "-"

        weekly_data.append({
            'week': week,
            'pick': pick_display,
            'game_result': game_result,
            'site': site,
            'result': result_str,
            'type': classification
        })

    return weekly_data, fav_count, dog_count, pickem_count


def emit_emails_sent_metric(week, emails_sent_count, year):
    retval = False
    try:
        cloudwatch.put_metric_data(
            Namespace='lotw',
            MetricData=[
                {
                    'MetricName': 'StandingsEmailsSent',
                    'Dimensions': [
                        {'Name': 'Year', 'Value': str(year)},
                        {'Name': 'Week', 'Value': str(week)}
                    ],
                    'Value': emails_sent_count,
                    'Unit': 'Count'
                },
            ]
        )
        logger.info("Emitted StandingsEmailsSent metric: %s", emails_sent_count)
        retval = True
    except Exception as e:
        logger.error("Failed to emit CloudWatch metric: %s", str(e))

    return retval


def lambda_handler(event, context):
    """
    Email LOTW standings to each player each week
    """
    logger.info("Received event: %s", json.dumps(event, indent=2))

    request_type = event.get('detail-type')
    if request_type is None:
        logger.error("Unable to determine request type")
        sys.exit()

    try:
        conn = get_db_connection()
    except Exception as e:
        logger.error("ERROR: Could not connect to MySQL database: %s", str(e))
        sys.exit()

    logger.info("SUCCESS: Connection to MySQL database succeeded")

    # --- Validate Mode & Determine Target Year ---
    current_year = get_current_year()
    year_env = os.environ.get('year')
    target_year = current_year

    if year_env is not None and year_env != '':
        try:
            parsed_year = int(year_env)
        except ValueError:
            conn.close()
            error_msg = f"Invalid year value: '{year_env}' is not an integer."
            logger.error(error_msg)
            return response(400, 'text/html', build_html(error_msg))

        # Only allow specifying a year if running as 'test'
        if request_type != "test":
            conn.close()
            error_msg = f"Specifying a custom year is only permitted when running as 'test'. Found detail-type: '{request_type}'."
            logger.error(error_msg)
            return response(400, 'text/html', build_html(error_msg))

        if not (2000 < parsed_year <= current_year):
            conn.close()
            error_msg = f"Invalid year: {parsed_year}. Year must be > 2000 and <= current year ({current_year})."
            logger.error(error_msg)
            return response(400, 'text/html', build_html(error_msg))

        target_year = parsed_year
    else:
        target_year = current_year

    # Enforce manual_run only for current year
    if request_type == "manual_run" and target_year != current_year:
        conn.close()
        error_msg = f"manual_run is only allowed for the current year ({current_year})."
        logger.error(error_msg)
        return response(400, 'text/html', build_html(error_msg))

    logger.info("Operating on target year: %s (Current Year: %s)", target_year, current_year)

    # Configuration
    mail_username = os.environ['mail_username']
    mail_password = os.environ['mail_password']
    mail_host = os.environ['mail_host']
    mail_port = os.environ['mail_port']
    mail_from = '"Brendan Connell" <bmoney312@gmail.com>'

    try:
        MAX_RETRIES = int(os.environ.get('SMTP_RETRIES', 5))
    except ValueError:
        MAX_RETRIES = 5

    try:
        RETRY_SLEEP_SECONDS = int(os.environ.get('SMTP_RETRY_SLEEP', 15))
    except ValueError:
        RETRY_SLEEP_SECONDS = 15

    player_id = os.environ.get('player_id')
    start_with_player_id = os.environ.get('start_with_player_id')

    if start_with_player_id:
        start_with_player_id = int(start_with_player_id)
        logger.info("Starting with player_id %s", start_with_player_id)

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
        logger.error("Invalid request type %s", request_type)
        conn.close()
        sys.exit()

    logger.info("Request type is %s", request_type)
    logger.debug("Players %s", players)

    standings_week = 0
    week = os.environ.get('week')

    if week is None:
        if target_year < current_year:
            # For past test seasons default to final week
            standings_week = 21 if target_year < 2021 else 22
            week = standings_week
        else:
            week = get_current_week(conn)
            if week is None:
                logger.error("ERROR: Unable to determine current week!")
                conn.close()
                sys.exit()
            standings_week = int(week) - 1
    else:
        standings_week = int(week)

    logger.info("Current week set to %s", week)
    logger.info("Standings week set to %s", standings_week)
    logger.info("Current time is %s", datetime.datetime.now())

    # Get standings for target year
    standings = get_standings_by_year(conn, target_year)
    total_players_season = len(standings)

    # Commissioner message logic: skipped for previous years
    if target_year < current_year:
        logger.info("Target year %s is prior to current year %s. Skipping commissioner message.", target_year, current_year)
        commish_message = ""
    else:
        if standings_week == 0:
            commish_message = 'Testing. Week 0 Standings.<br>'
        else:
            commish_message = get_standings_message_by_year(conn, standings_week, target_year)

        if commish_message is None:
            if request_type == "test":
                commish_message = f"Testing standings for week {standings_week}.<br>"
            else:
                logger.error("Unexpected missing value for commish message")
                conn.close()
                sys.exit()

    picks_map = {}
    player_picks_by_player = {}
    games_by_week_team = {}

    with conn.cursor() as cur:
        picks_table = f"Picks_{target_year}"
        games_table = f"Games_{target_year}"

        cur.execute(f"""
            SELECT p.player_id, p.pick,
                   CASE
                       WHEN p.pick = g.home_team_id THEN g.home_team_line
                       WHEN p.pick = g.away_team_id THEN -g.home_team_line
                       ELSE NULL
                   END AS line,
                   p.pick_ats,
                   (CURRENT_TIMESTAMP >= p.lock_in_time) AS locked_in
            FROM {picks_table} p
            LEFT JOIN {games_table} g ON g.week = %s AND (p.pick = g.home_team_id OR p.pick = g.away_team_id)
            WHERE p.week = %s AND p.lock_in_time IS NOT NULL
            ORDER BY p.submit_time DESC
        """, (standings_week, standings_week))

        for row in cur.fetchall():
            pid, p_pick, p_line, p_ats, p_locked = row
            if pid not in picks_map:
                picks_map[pid] = (p_pick, p_line, p_ats, bool(p_locked))

        if standings_week > 1 or request_type == "test":
            cur.execute(f"""
                SELECT home_team_id, away_team_id, home_team_line, away_team_score, home_team_score, week
                FROM {games_table}
            """)
            for h_id, a_id, h_line, a_score, h_score, g_week in cur.fetchall():
                game_data = (h_id, a_id, h_line, a_score, h_score)
                games_by_week_team[(g_week, h_id)] = game_data
                games_by_week_team[(g_week, a_id)] = game_data

            cur.execute(f"""
                SELECT player_id, week, pick, pick_ats
                FROM {picks_table}
                WHERE lock_in_time IS NOT NULL AND lock_in_time <= CURRENT_TIMESTAMP
                ORDER BY week ASC
            """)
            for pid, p_week, p_pick, p_ats in cur.fetchall():
                if pid not in player_picks_by_player:
                    player_picks_by_player[pid] = []
                player_picks_by_player[pid].append((p_week, p_pick, p_ats))

    emails_sent_count = 0
    smtp_relay = smtp_connect(mail_host, mail_port, mail_username, mail_password)

    if smtp_relay is None:
        logger.error("Error establishing SMTP connection with %s", mail_host)
        conn.close()
        sys.exit()

    for player in players:
        (player_id, player_email, last_name, first_name, titles, is_rookie) = player
        logger.info("Working on player %s %s %s %s", player_id, first_name, last_name, player_email)

        if start_with_player_id is not None and request_type != "test":
            if player_id < start_with_player_id:
                logger.info("Skipping player %s which is less than start_with_player_id %s", player_id, start_with_player_id)
                continue

        message = "<br>"

        if standings_week > 1 or request_type == "test":
            logger.info("Building pick report for player %s", player_id)
            message = message + "<br><h3>Your picks:</h3>\n"

            weekly_data, season_fav, season_dog, season_pickem = get_player_season_details_cached(
                player_id, player_picks_by_player, games_by_week_team
            )

            season_wins = 0
            season_losses = 0
            rank = "-"

            for i, row in enumerate(standings):
                if row[0] == player_id:
                    rank = i + 1
                    season_wins = row[5]
                    season_losses = row[6]
                    break

            season_total = season_wins + season_losses
            season_pct = (season_wins / season_total * 100) if season_total > 0 else 0.0

            message += f"<b>Record:</b> {season_wins}-{season_losses} ({season_pct:.1f}%)<br>"
            message += f"<b>Current Rank:</b> {rank} of {total_players_season}<br>"
            message += f"<b>Tendencies:</b> {season_fav} Favorites / {season_dog} Underdogs / {season_pickem} Pick &apos;em<br><br>"

            message += "<table><tr><th>Week</th><th>Pick</th><th>Game Result</th><th>Site</th><th>Type</th><th>Result</th></tr>"
            for row in weekly_data:
                res_class = "win" if row['result'] == "Win" else ("loss" if (row['result'] == "Loss" or row['result'] == "Loss (Push)") else "")
                message += f"<tr><td>{row['week']}</td><td>{row['pick']}</td><td>{row['game_result']}</td><td>{row['site']}</td><td>{row['type']}</td><td class='{res_class}'>{row['result']}</td></tr>"
            message += "</table><br>\n"

        standings_html = get_standings_html(standings_week, standings, player_id, picks_map)
        mail_body = f"{build_html_head()}\n<body>\n{commish_message}{message}{standings_html}<br></body></html>"
        mail_to = (player_email, 'bmoney312@gmail.com')

        # Determine email subject
        if target_year < current_year:
            mail_subject = f"lotw: final standings ({target_year}-{target_year + 1})"
        else:
            mail_subject = f"lotw: week {standings_week} standings"
            if standings_week == 19:
                mail_subject = f"lotw: week {standings_week} standings (wildcard weekend)"
            elif standings_week == 20:
                mail_subject = f"lotw: week {standings_week} standings (divisional playoffs)"
            elif standings_week == 21:
                mail_subject = f"lotw: week {standings_week} standings (conference championships)"
            elif standings_week == 22:
                mail_subject = f"lotw: week {standings_week} standings (super bowl)"

        email_sent_successfully = False
        for attempt in range(MAX_RETRIES):
            email_result = smtp_send(smtp_relay, mail_subject, mail_body, mail_to, mail_from)

            if email_result is True:
                logger.info("Email sent successfully to player %s %s on attempt %s", player_id, player_email, attempt + 1)
                email_sent_successfully = True
                emails_sent_count += 1
                break
            else:
                logger.error("Email failed to player %s %s on attempt %s", player_id, player_email, attempt + 1)
                if attempt <= MAX_RETRIES:
                    logger.info("Sleeping for %s seconds before retry...", RETRY_SLEEP_SECONDS)
                    smtp_relay.close()
                    sleep(RETRY_SLEEP_SECONDS)

                    smtp_relay = None
                    smtp_relay = smtp_connect(mail_host, mail_port, mail_username, mail_password)

                    if smtp_relay is None:
                        logger.error("Error re-establishing SMTP connection with %s. Stopping retries for this player.", mail_host)
                        break
                else:
                    logger.error("All %s retry attempts failed for player %s %s", MAX_RETRIES, player_id, player_email)

        if not email_sent_successfully:
            logger.error("Aborting email send for player %s %s after all retries.", player_id, player_email)
            if smtp_relay is not None:
                smtp_relay.close()

            emit_emails_sent_metric(standings_week, emails_sent_count, target_year)
            conn.close()
            raise RuntimeError(f"Standings for week {standings_week} send failed for player {player_id} after {MAX_RETRIES} attempts. Aborting.")

        sleep(2)

    emit_emails_sent_metric(standings_week, emails_sent_count, target_year)
    conn.close()
    smtp_relay.close()
    logger.info("Standings for week %s year %s sent successfully to %s players.", standings_week, target_year, emails_sent_count)
    return response(200, 'text/html', build_html(f"Standings for week {standings_week} year {target_year} sent successfully to {emails_sent_count} players."))
