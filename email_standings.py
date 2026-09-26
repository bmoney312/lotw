import os
import sys
import json
import logging
import datetime
import boto3
from time import sleep
from lotw import get_all_paid_players, get_player, get_standings, get_standings_full_name
from lotw import get_current_week, get_standings_message, get_current_year, get_db_connection
from lotw import build_html, formatted_line, response, smtp_send, smtp_connect

# global variables
logger = logging.getLogger()
logger.setLevel(logging.INFO)
cloudwatch = boto3.client('cloudwatch')


def get_standings_for_year(conn, year):
    """
    Return LOTW standings for the specified year with player names and attributes.
    """
    with conn.cursor() as cur:
        select_statement = f"""
            SELECT s.player_id, p.last_name, p.first_name, p.past_titles,
                   p.rookie, s.wins, s.losses, s.win_percentage, s.ats_points, s.streak
            FROM `Standings_{year}` s
            INNER JOIN `Players` p ON s.player_id = p.player_id
            WHERE p.`{year}_registration` = 1
            ORDER BY s.win_percentage DESC, s.ats_points DESC, p.last_name ASC, p.first_name ASC
        """
        cur.execute(select_statement)
        return cur.fetchall()


def build_standings_email_head():
    """
    Build email head with left-justified header and centered card container styling.
    """
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
         table.email-table {
             width: 100%;
             max-width: 650px;
             margin: 0 auto;
             border-collapse: collapse;
             border: 1px solid black;
         }
         table.email-table th {
             border: 1px solid black;
             padding: 6px;
             text-align: left;
             background-color: lightgrey;
         }
         table.email-table td {
             border: 1px solid black;
             padding: 6px;
             text-align: left;
         }
         .win { color: green; font-weight: bold; }
         .loss { color: red; }
         .footer-logo {
             text-align: center;
             margin-top: 25px;
             margin-bottom: 15px;
         }
    </style>
</head>
"""
    return html


def get_standings_html(week, standings, current_player_id, picks_map):
    """
    Return string of LOTW standings in centered HTML table with left-justified header
    """
    if week == 19:
        html = '<br><br><h3 style="text-align: left;">LOTW: WEEK {} STANDINGS (WILDCARD WEEKEND)</h3>\n'.format(week)
    elif week == 20:
        html = '<br><br><h3 style="text-align: left;">LOTW: WEEK {} STANDINGS (DIVISIONAL PLAYOFFS)</h3>\n'.format(week)
    elif week == 21:
        html = '<br><br><h3 style="text-align: left;">LOTW: WEEK {} STANDINGS (CONFERENCE CHAMPIONSHIPS)</h3>\n'.format(week)
    elif week == 22:
        html = '<br><br><h3 style="text-align: left;">LOTW: WEEK {} STANDINGS (SUPER BOWL)</h3>\n'.format(week)
    else:
        html = '<br><br><h3 style="text-align: left;">LOTW: WEEK {} STANDINGS</h3>\n'.format(week)

    html += """
<table class="email-table" role="presentation" border="1" cellpadding="6" cellspacing="0" align="center" style="margin: 0 auto; border-collapse: collapse; width: 100%;">
<tr>
    <th style="background-color: lightgrey; border: 1px solid black; padding: 6px; text-align: left;">Rank</th>
    <th style="background-color: lightgrey; border: 1px solid black; padding: 6px; text-align: left;">Name</th>
    <th style="background-color: lightgrey; border: 1px solid black; padding: 6px; text-align: left;">Wins</th>
    <th style="background-color: lightgrey; border: 1px solid black; padding: 6px; text-align: left;">Losses</th>
    <th style="background-color: lightgrey; border: 1px solid black; padding: 6px; text-align: left;">Win %</th>
    <th style="background-color: lightgrey; border: 1px solid black; padding: 6px; text-align: left;">ATS Points</th>
    <th style="background-color: lightgrey; border: 1px solid black; padding: 6px; text-align: left;">Streak</th>
    <th style="background-color: lightgrey; border: 1px solid black; padding: 6px; text-align: left;">Week {} Pick</th>
    <th style="background-color: lightgrey; border: 1px solid black; padding: 6px; text-align: left;">Week {} Result</th>
</tr>
""".format(week, week)

    rank = 1
    for row in standings:
        (player_id, last_name, first_name, past_titles, rookie, wins, losses, win_percentage, ats_points, streak) = row
        full_name = get_standings_full_name(first_name, last_name, past_titles, rookie)
        pick_data = picks_map.get(player_id, ("NOP", None, None, False))
        (pick, line, pick_ats, locked_in) = pick_data

        highlight_row = False
        if player_id == current_player_id:
            highlight_row = True

        if pick == "NOP" and pick_ats is None:
            pick_ats = 0
            locked_in = True
            pick_as_string = "NO PICK"
        else:
            pick_as_string = "{} {}".format(pick, formatted_line(line))

        if pick_ats is not None and pick_ats > 0:
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

        html += build_standings_html_row(rank, full_name, wins, losses, win_percentage_string, ats_points, streak, pick_as_string, result, highlight_row)
        rank += 1

    html += "</table>"
    html += """
          </td>
        </tr>
      </table>
      <!-- End Centered Card Container -->
    </td>
  </tr>
</table>
</body></html>"""
    return html


def build_standings_html_row(rank, full_name, wins, losses, win_percentage, ats_points, streak, pick_as_string, result, highlight_row):
    """
    Build HTML string of single row in standings with explicit inline styles for email client compatibility
    """
    cell_style = "border: 1px solid black; padding: 6px; text-align: left;"
    if highlight_row is True:
        html = """
<tr>
<td style="{}"><b>{}</b></td>
<td style="{}"><b>{}</b></td>
<td style="{}"><b>{}</b></td>
<td style="{}"><b>{}</b></td>
<td style="{}"><b>{}</b></td>
<td style="{}"><b>{}</b></td>
<td style="{}"><b>{}</b></td>
<td style="{}"><b>{}</b></td>
<td style="{}"><b>{}</b></td>
</tr>""".format(cell_style, rank, cell_style, full_name, cell_style, wins, cell_style, losses, cell_style, win_percentage, cell_style, ats_points, cell_style, streak, cell_style, pick_as_string, cell_style, result)
    else:
        html = """
<tr>
<td style="{}">{}</td>
<td style="{}">{}</td>
<td style="{}">{}</td>
<td style="{}">{}</td>
<td style="{}">{}</td>
<td style="{}">{}</td>
<td style="{}">{}</td>
<td style="{}">{}</td>
<td style="{}">{}</td>
</tr>""".format(cell_style, rank, cell_style, full_name, cell_style, wins, cell_style, losses, cell_style, win_percentage, cell_style, ats_points, cell_style, streak, cell_style, pick_as_string, cell_style, result)

    return html


def get_player_season_details_cached(player_id, player_picks_by_player, games_by_week_team):
    """
    Get weekly breakdown for current year: Week, Pick, Game Result, Site, Result, Fav/Dog status.
    Uses in-memory dictionaries to eliminate N+1 DB calls.
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
                game_result = "{} {} v {} {}".format(away_team_id, away_score, home_team_id, home_score)

        pick_display = "{} {}".format(pick, formatted_line(line)) if line is not None else pick

        if pick_ats is not None:
            if pick_ats > 0:
                result_str = "Win (+{} ATS)".format(pick_ats)
            elif pick_ats < 0:
                result_str = "Loss ({} ATS)".format(pick_ats)
            else:
                result_str = "Loss (Push) (0 ATS)"
        else:
            result_str = "-"

        weekly_data.append({
            'week': week,
            'pick': pick_display,
            'game_result': game_result,
            'site': site,
            'result': result_str,
            'pick_ats': pick_ats,
            'type': classification
        })

    return weekly_data, fav_count, dog_count, pickem_count


def emit_emails_sent_metric(week, emails_sent_count, request_type=None, year=None):
    if year is None:
        year = get_current_year()

    retval = False
    metric_data = [
        {
            'MetricName': 'StandingsEmailsSent',
            'Dimensions': [
                {'Name': 'Year', 'Value': str(year)},
                {'Name': 'Week', 'Value': str(week)}
            ],
            'Value': emails_sent_count,
            'Unit': 'Count'
        }
    ]

    if request_type == "Scheduled Event":
        metric_data.append({
            'MetricName': 'ScheduledStandingsEmailsSent',
            'Dimensions': [
                {'Name': 'Year', 'Value': str(year)},
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
            "Emitted metric(s) for count %s (scheduled=%s, year=%s)",
            emails_sent_count,
            request_type == "Scheduled Event",
            year
        )
        retval = True
    except Exception as e:
        logger.error("Failed to emit CloudWatch metric: {}".format(str(e)))

    return retval


def lambda_handler(event, context):
    """
    Email LOTW standings to each player each week
    """
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
        logger.info("Starting with player_id {}".format(start_with_player_id))

    # Determine target year
    current_year = get_current_year()
    query_params = event.get('queryStringParameters') or {}
    year_param = event.get('year') or os.environ.get('year') or query_params.get('year')

    if year_param is not None and str(year_param).strip() != '':
        try:
            year = int(year_param)
        except ValueError:
            error_msg = "Invalid year value: '{}' is not an integer".format(year_param)
            logger.error(error_msg)
            conn.close()
            return response(400, 'text/html', build_html(error_msg))

        if not (2000 < year <= current_year):
            error_msg = "Invalid year: {}. Year must be > 2000 and <= current year ({})".format(year, current_year)
            logger.error(error_msg)
            conn.close()
            return response(400, 'text/html', build_html(error_msg))
    else:
        year = current_year

    # Add target year to INFO log
    logger.info("Target year set to {}".format(year))

    is_past_year = year < current_year

    # Only allow sending of past year standings as a test event
    if is_past_year and request_type != "test":
        error_msg = "Error: Sending past year standings ({}) is only allowed as a test event (request_type='test').".format(year)
        logger.error(error_msg)
        conn.close()
        return response(400, 'text/html', build_html(error_msg))

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
        conn.close()
        sys.exit()

    logger.info("Request type is {}".format(request_type))
    logger.debug("Players {}".format(players))

    standings_week = 0

    # Automatically set week for past years (week 21 for <= 2020, week 22 for 2021+)
    if is_past_year:
        standings_week = 21 if year <= 2020 else 22
        week = standings_week
        logger.info("Past year {} detected. Automatically setting final standings week to {}".format(year, standings_week))
    else:
        week = os.environ.get('week')
        if week is None:
            week = get_current_week(conn)
            if week is None:
                logger.error("ERROR: Unable to determine current week!")
                conn.close()
                sys.exit()
            standings_week = int(week) - 1
        else:
            standings_week = int(week)

    logger.info("Current week set to {}".format(week))
    logger.info("Standings week set to {}".format(standings_week))
    logger.info("Current time is {}".format(datetime.datetime.now()))

    # Fetch standings for target year
    if is_past_year:
        standings = get_standings_for_year(conn, year)
    else:
        standings = get_standings(conn)

    total_players_season = len(standings)

    if standings_week == 0:
        commish_message = 'Testing. Week 0 Standings.<br>'
    elif is_past_year:
        commish_message = "Testing final standings for the {} season (Week {}).<br>".format(year, standings_week)
    else:
        commish_message = get_standings_message(conn, standings_week)

    if commish_message is None:
        if request_type == "test":
            commish_message = "Testing standings for week {}.<br>".format(standings_week)
        else:
            logger.error("Unexpected missing value for commish message")
            conn.close()
            sys.exit()

    # --- Pre-fetch Current Week Picks and Season Game Results ---
    picks_map = {}
    player_picks_by_player = {}
    games_by_week_team = {}
    weekly_games = []

    with conn.cursor() as cur:
        picks_table = "Picks_{}".format(year)
        games_table = "Games_{}".format(year)

        cur.execute("""
            SELECT p.player_id, p.pick,
                   CASE
                       WHEN p.pick = g.home_team_id THEN g.home_team_line
                       WHEN p.pick = g.away_team_id THEN -g.home_team_line
                       ELSE NULL
                   END AS line,
                   p.pick_ats,
                   (CURRENT_TIMESTAMP >= p.lock_in_time) AS locked_in
            FROM {} p
            LEFT JOIN {} g ON g.week = %s AND (p.pick = g.home_team_id OR p.pick = g.away_team_id)
            WHERE p.week = %s AND p.lock_in_time IS NOT NULL
            ORDER BY p.submit_time DESC
        """.format(picks_table, games_table), (standings_week, standings_week))

        for row in cur.fetchall():
            pid, p_pick, p_line, p_ats, p_locked = row
            if pid not in picks_map:
                picks_map[pid] = (p_pick, p_line, p_ats, bool(p_locked))

        cur.execute("""
            SELECT home_team_id, away_team_id, home_team_line, away_team_score, home_team_score, week
            FROM {}
        """.format(games_table))
        for h_id, a_id, h_line, a_score, h_score, g_week in cur.fetchall():
            game_data = (h_id, a_id, h_line, a_score, h_score)
            games_by_week_team[(g_week, h_id)] = game_data
            games_by_week_team[(g_week, a_id)] = game_data
            if g_week == standings_week:
                weekly_games.append(game_data)

        cur.execute("""
            SELECT player_id, week, pick, pick_ats
            FROM {}
            WHERE lock_in_time IS NOT NULL AND lock_in_time <= CURRENT_TIMESTAMP
            ORDER BY week ASC
        """.format(picks_table))
        for pid, p_week, p_pick, p_ats in cur.fetchall():
            if pid not in player_picks_by_player:
                player_picks_by_player[pid] = []
            player_picks_by_player[pid].append((p_week, p_pick, p_ats))

    # --- Calculate Weekly Trends ---
    field_wins = 0
    pick_counts = {}
    pick_ats_map = {}

    for pid, pick_data in picks_map.items():
        p_pick, p_line, p_ats, p_locked = pick_data
        if p_pick != "NOP" and p_ats is not None:
            pick_counts[p_pick] = pick_counts.get(p_pick, 0) + 1
            pick_ats_map[p_pick] = p_ats
            if p_ats > 0:
                field_wins += 1

    field_losses = total_players_season - field_wins
    field_pct = (field_wins / total_players_season * 100) if total_players_season > 0 else 0.0

    fav_wins = 0
    fav_losses = 0
    dog_wins = 0
    dog_losses = 0

    for h_id, a_id, h_line, a_score, h_score in weekly_games:
        if h_line is not None and a_score is not None and h_score is not None and h_line != 0:
            if h_line < 0:
                fav_ats = h_line + (h_score - a_score)
            else:
                fav_ats = -h_line + (a_score - h_score)

            dog_ats = -fav_ats

            if fav_ats > 0:
                fav_wins += 1
            else:
                fav_losses += 1

            if dog_ats > 0:
                dog_wins += 1
            else:
                dog_losses += 1

    fav_total = fav_wins + fav_losses
    fav_pct = (fav_wins / fav_total * 100) if fav_total > 0 else 0.0

    dog_total = dog_wins + dog_losses
    dog_pct = (dog_wins / dog_total * 100) if dog_total > 0 else 0.0

    winning_picks = {team: count for team, count in pick_counts.items() if pick_ats_map[team] > 0}
    losing_picks = {team: count for team, count in pick_counts.items() if pick_ats_map[team] <= 0}

    def get_most_picked(picks_dict):
        if not picks_dict:
            return "-"
        max_count = max(picks_dict.values())
        top_teams = [team for team, count in picks_dict.items() if count == max_count]
        return "{} ({} picks)".format(", ".join(top_teams), max_count)

    most_picked_win = get_most_picked(winning_picks)
    most_picked_loss = get_most_picked(losing_picks)

    if pick_ats_map:
        max_ats = max(pick_ats_map.values())
        best_teams = [team for team, ats in pick_ats_map.items() if ats == max_ats]
        best_ats_str = "+{}".format(max_ats) if max_ats > 0 else str(max_ats)
        best_pick_str = "{} ({} ATS Points)".format(", ".join(best_teams), best_ats_str)

        min_ats = min(pick_ats_map.values())
        worst_teams = [team for team, ats in pick_ats_map.items() if ats == min_ats]
        worst_ats_str = "+{}".format(min_ats) if min_ats > 0 else str(min_ats)
        worst_pick_str = "{} ({} ATS Points)".format(", ".join(worst_teams), worst_ats_str)
    else:
        best_pick_str = "-"
        worst_pick_str = "-"

    trends_html = '<h3 style="text-align: left;">Trends this week:</h3>\n'
    trends_html += "<b>Field record:</b> {}-{} ({:.1f}%)<br>\n".format(field_wins, field_losses, field_pct)
    trends_html += "<b>Favorites:</b> {}-{} ({:.1f}%)<br>\n".format(fav_wins, fav_losses, fav_pct)
    trends_html += "<b>Underdogs:</b> {}-{} ({:.1f}%)<br>\n".format(dog_wins, dog_losses, dog_pct)
    trends_html += "<b>Most picked win:</b> {}<br>\n".format(most_picked_win)
    trends_html += "<b>Most picked loss:</b> {}<br>\n".format(most_picked_loss)
    trends_html += "<b>Best pick:</b> {}<br>\n".format(best_pick_str)
    trends_html += "<b>Worst pick:</b> {}<br>\n".format(worst_pick_str)

    emails_sent_count = 0
    smtp_relay = smtp_connect(mail_host, mail_port, mail_username, mail_password)

    if smtp_relay is None:
        logger.error("Error establishing SMTP connection with {}".format(mail_host))
        conn.close()
        sys.exit()

    for player in players:
        (player_id, player_email, last_name, first_name, titles, is_rookie) = player
        logger.info("Working on player {} {} {} {}".format(player_id, first_name, last_name, player_email))

        if start_with_player_id is not None and request_type != "test":
            if player_id < start_with_player_id:
                logger.info("Skipping player {} which is less than start_with_player_id {}".format(player_id, start_with_player_id))
                continue

        # Open body and centered container card
        message = """<body>
<table role="presentation" border="0" cellpadding="0" cellspacing="0" width="100%" style="background-color: #f6f6f6;">
  <tr>
    <td align="center" style="padding: 20px 10px;">
      <!-- Centered Card Container -->
      <table role="presentation" border="0" cellpadding="0" cellspacing="0" width="100%" style="max-width: 650px; background-color: #ffffff; border-radius: 6px; padding: 20px;">
        <tr>
          <td>
"""
        message += commish_message + "<br>"
        message += trends_html

        logger.info("Building pick report for player {}".format(player_id))
        message += '<br><h3 style="text-align: left;">Your picks:</h3>\n'

        weekly_data, season_fav, season_dog, season_pickem = get_player_season_details_cached(
            player_id, player_picks_by_player, games_by_week_team
        )

        season_wins = 0
        season_losses = 0
        season_ats = 0
        rank = "-"

        for i, row in enumerate(standings):
            if row[0] == player_id:
                rank = i + 1
                season_wins = row[5]
                season_losses = row[6]
                season_ats = row[8]
                break

        season_total = season_wins + season_losses
        season_pct = (season_wins / season_total * 100) if season_total > 0 else 0.0

        message += "<b>Record:</b> {}-{} ({:.1f}%) ({} ATS Points)<br>".format(season_wins, season_losses, season_pct, season_ats)
        message += "<b>Current Rank:</b> {} of {}<br>".format(rank, total_players_season)
        message += "<b>Tendencies:</b> {} Favorites / {} Underdogs / {} Pick &apos;em<br><br>".format(season_fav, season_dog, season_pickem)

        cell_style = "border: 1px solid black; padding: 6px; text-align: left;"
        message += """
<table class="email-table" role="presentation" border="1" cellpadding="6" cellspacing="0" align="center" style="margin: 0 auto; border-collapse: collapse; width: 100%;">
<tr>
    <th style="background-color: lightgrey; border: 1px solid black; padding: 6px; text-align: left;">Week</th>
    <th style="background-color: lightgrey; border: 1px solid black; padding: 6px; text-align: left;">Pick</th>
    <th style="background-color: lightgrey; border: 1px solid black; padding: 6px; text-align: left;">Game Result</th>
    <th style="background-color: lightgrey; border: 1px solid black; padding: 6px; text-align: left;">Site</th>
    <th style="background-color: lightgrey; border: 1px solid black; padding: 6px; text-align: left;">Type</th>
    <th style="background-color: lightgrey; border: 1px solid black; padding: 6px; text-align: left;">Result</th>
</tr>"""
        for row in weekly_data:
            if row['pick_ats'] is not None:
                res_class = "win" if row['pick_ats'] > 0 else "loss"
            else:
                res_class = ""

            message += """
<tr>
    <td style="{}">{}</td>
    <td style="{}">{}</td>
    <td style="{}">{}</td>
    <td style="{}">{}</td>
    <td style="{}">{}</td>
    <td style="{}" class="{}">{}</td>
</tr>""".format(
                cell_style, row['week'],
                cell_style, row['pick'],
                cell_style, row['game_result'],
                cell_style, row['site'],
                cell_style, row['type'],
                cell_style, res_class, row['result']
            )
        message += "</table><br>\n"

        standings_html = get_standings_html(standings_week, standings, player_id, picks_map)
        mail_body = build_standings_email_head() + "\n" + message + standings_html
        mail_to = (player_email, 'bmoney312@gmail.com')

        # Subject line logic
        if is_past_year:
            mail_subject = "lotw: final standings {} - {}".format(year, year + 1)
        else:
            mail_subject = "lotw: week {} standings".format(standings_week)
            if standings_week == 19:
                mail_subject = "lotw: week {} standings (wildcard weekend)".format(standings_week)
            elif standings_week == 20:
                mail_subject = "lotw: week {} standings (divisional playoffs)".format(standings_week)
            elif standings_week == 21:
                mail_subject = "lotw: week {} standings (conference championships)".format(standings_week)
            elif standings_week == 22:
                mail_subject = "lotw: week {} standings (super bowl)".format(standings_week)

        email_sent_successfully = False
        for attempt in range(MAX_RETRIES):
            email_result = smtp_send(smtp_relay, mail_subject, mail_body, mail_to, mail_from)

            if email_result is True:
                logger.info("Email sent successfully to player {} {} on attempt {}".format(player_id, player_email, attempt + 1))
                email_sent_successfully = True
                emails_sent_count += 1
                break
            else:
                logger.error("Email failed to player {} {} on attempt {}".format(player_id, player_email, attempt + 1))
                if attempt <= MAX_RETRIES:
                    logger.info("Sleeping for {} seconds before retry...".format(RETRY_SLEEP_SECONDS))
                    smtp_relay.close()
                    sleep(RETRY_SLEEP_SECONDS)

                    smtp_relay = None
                    smtp_relay = smtp_connect(mail_host, mail_port, mail_username, mail_password)

                    if smtp_relay is None:
                        logger.error("Error re-establishing SMTP connection with {}. Stopping retries for this player.".format(mail_host))
                        break
                else:
                    logger.error("All {} retry attempts failed for player {} {}".format(MAX_RETRIES, player_id, player_email))

        if not email_sent_successfully:
            logger.error("Aborting email send for player {} {} after all retries.".format(player_id, player_email))

            if smtp_relay is None:
                logger.error("SMTP connection is dead.")
            else:
                logger.info("Closing connection to SMTP relay.")
                smtp_relay.close()

            emit_emails_sent_metric(standings_week, emails_sent_count, request_type, year=year)
            conn.close()

            logger.info("Standings for week {} send failed for player {} after {} attempts. Aborting.".format(standings_week, player_id, MAX_RETRIES))
            raise RuntimeError("Standings for week {} send failed for player {} after {} attempts. Aborting.".format(standings_week, player_id, MAX_RETRIES))

        sleep(2)

    emit_emails_sent_metric(standings_week, emails_sent_count, request_type, year=year)
    conn.close()
    smtp_relay.close()
    logger.info("Standings for week {} (year {}) sent successfully to {} players.".format(standings_week, year, emails_sent_count))
    return response(200, 'text/html', build_html("Standings for week {} (year {}) sent successfully to {} players.".format(standings_week, year, emails_sent_count)))
