import os
import sys
import json
import pymysql
import logging
import datetime
from time import sleep
from lotw import get_current_year, get_current_week, get_all_paid_players, get_player, get_team_name
from lotw import build_html_head, response, smtp_connect, smtp_send, get_standings, formatted_line, build_html

# global variables
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- Helper Functions ---
def get_pick_details(conn, pick_team_id, week, year):
    """
    Determine the classification (Favorite/Underdog) and the specific line 
    for the picked team.
    Returns: (Classification, Line) 
             e.g. ('Favorite', -3.5) or (None, None)
    """
    with conn.cursor() as cur:
        # Get game details for the pick
        table_name = "Games_{}".format(year)
        sql = """
            SELECT home_team_id, away_team_id, home_team_line 
            FROM {} 
            WHERE week = %s AND (home_team_id = %s OR away_team_id = %s)
        """.format(table_name)
        cur.execute(sql, (week, pick_team_id, pick_team_id))
        row = cur.fetchone()
        
        if not row:
            return None, None

        home_team, away_team, home_line = row
        
        if home_line is None:
            return None, None
            
        # Calculate line from the perspective of the PICKED team
        # home_team_line is relative to Home (e.g. -3 means Home is favored)
        if pick_team_id == home_team:
            relevant_line = home_line
        else:
            relevant_line = -home_line
            
        # Determine Classification
        if relevant_line < 0: 
            cls = "Favorite"
        elif relevant_line > 0: 
            cls = "Underdog"
        else: 
            cls = "Pick'em"

        return cls, relevant_line

def get_player_season_details(conn, player_id, year):
    """
    Get weekly breakdown for current year: Week, Pick, Result, Fav/Dog status.
    Appends the spread to the pick name.
    """
    picks_table = "Picks_{}".format(year)

    with conn.cursor() as cur:
        sql = """
            SELECT week, pick, pick_ats
            FROM {}
            WHERE player_id = %s
            AND lock_in_time IS NOT NULL
            AND lock_in_time <= CURRENT_TIMESTAMP
            ORDER BY week ASC
        """.format(picks_table)
        cur.execute(sql, (player_id,))
        rows = cur.fetchall()

    weekly_data = []
    fav_count = 0
    dog_count = 0
    push_count = 0

    for row in rows:
        week, pick, pick_ats = row

        # Get Classification and Line
        classification, line = get_pick_details(conn, pick, week, year)

        if classification == "Favorite":
            fav_count += 1
        elif classification == "Underdog":
            dog_count += 1
        elif classification == "Pick'em":
            push_count += 1

        # Format Pick String with Line (e.g., "DEN -3")
        if line is not None:
            pick_display = "{} {}".format(pick, formatted_line(line))
        else:
            pick_display = pick

        # Format Result
        result_str = "-"
        if pick_ats is not None:
            if pick_ats > 0: result_str = "Win"
            elif pick_ats < 0: result_str = "Loss"
            else: result_str = "Loss (Push)"

        weekly_data.append({
            'week': week,
            'pick': pick_display, # Use formatted string
            'result': result_str,
            'type': classification if classification else "-"
        })

    return weekly_data, fav_count, dog_count

def get_player_career_stats(conn, player_id, start_year, end_year):
    """
    Aggregate wins, losses, fav counts, dog counts from start_year to end_year.
    """
    total_wins = 0
    total_losses = 0
    total_fav = 0
    total_dog = 0

    for year in range(start_year, end_year + 1):
        try:
            picks_table = "Picks_{}".format(year)
            with conn.cursor() as cur:
                sql = """
                    SELECT week, pick, pick_ats
                    FROM {}
                    WHERE player_id = %s
                    AND lock_in_time IS NOT NULL
                    AND lock_in_time <= CURRENT_TIMESTAMP
                """.format(picks_table)
                cur.execute(sql, (player_id,))
                rows = cur.fetchall()

                for row in rows:
                    week, pick, pick_ats = row

                    # W/L Record
                    if pick_ats is not None:
                        if pick_ats > 0: total_wins += 1
                        elif pick_ats <= 0: total_losses += 1

                    # Fav/Dog Record
                    cls, _ = get_pick_details(conn, pick, week, year) # unpack tuple, ignore line
                    if cls == "Favorite": total_fav += 1
                    elif cls == "Underdog": total_dog += 1

        except Exception as e:
            continue

    return total_wins, total_losses, total_fav, total_dog

def get_team_ats_records(conn, year):
    """
    Calculate every NFL team's record against the spread for the current year.
    Returns list of tuples sorted by Win %: (Full Team Name, Wins, Losses, Win%)
    """
    games_table = "Games_{}".format(year)
    team_stats = {} # { 'TEAM_ID': {'w': 0, 'l': 0} }

    with conn.cursor() as cur:
        sql = "SELECT home_team_id, home_team_ats, away_team_id, away_team_ats FROM {} WHERE home_team_ats IS NOT NULL".format(games_table)
        cur.execute(sql)
        rows = cur.fetchall()

    for row in rows:
        home, home_ats, away, away_ats = row

        if home not in team_stats: team_stats[home] = {'w':0, 'l':0}
        if away not in team_stats: team_stats[away] = {'w':0, 'l':0}

        # Home Result
        if home_ats > 0: team_stats[home]['w'] += 1
        elif home_ats <= 0: team_stats[home]['l'] += 1

        # Away Result
        if away_ats > 0: team_stats[away]['w'] += 1
        elif away_ats <= 0: team_stats[away]['l'] += 1

    # Convert to list and sort
    results = []
    for team_id, stats in team_stats.items():
        w = stats['w']
        l = stats['l']
        total = w + l
        pct = (w / total) if total > 0 else 0.0

        # Resolve full name (e.g., "Denver Broncos")
        full_name = get_team_name(conn, team_id)
        if full_name is None:
            full_name = team_id # Fallback

        results.append((full_name, w, l, pct))

    # Sort by Win % desc, then Wins desc
    results.sort(key=lambda x: (x[3], x[1]), reverse=True)
    return results

def get_all_career_standings(conn, start_year, end_year):
    """
    Aggregate career records for ALL players since start_year.
    Returns list of tuples: (Player ID, Player Name, Wins, Losses, Win%)
    """
    # First get all players
    with conn.cursor() as cur:
        cur.execute("SELECT player_id, first_name, last_name FROM Players")
        players = cur.fetchall()

    career_standings = []

    for player in players:
        p_id, fname, lname = player
        w, l, f, d = get_player_career_stats(conn, p_id, start_year, end_year)

        total = w + l
        # Filter: Omit players with fewer than 42 career picks (two seasons)
        if total >= 42:
            pct = w / total
            full_name = "{} {}".format(fname, lname)
            # Added p_id to tuple at index 0
            career_standings.append((p_id, full_name, w, l, pct))

    # Sort by Win % (index 4), then Wins (index 2)
    career_standings.sort(key=lambda x: (x[4], x[2]), reverse=True)
    return career_standings

# --- HTML Builders ---

def build_analytics_html(
    first_name, 
    current_year,
    weekly_data, 
    season_fav, season_dog, 
    season_wins, season_losses, rank, total_players_season,
    career_wins, career_losses, career_fav, career_dog,
    team_ats_records,
    all_career_standings,
    current_player_id
):
    """
    Construct the full HTML body for the email.
    """
    style = """
    <style>
        table { border-collapse: collapse; width: 100%; max_width: 600px; margin-bottom: 20px; }
        th { background-color: #f2f2f2; border: 1px solid #ddd; padding: 8px; text-align: left; }
        td { border: 1px solid #ddd; padding: 8px; }
        h3 { color: #333; margin-top: 25px; }
        h4 { color: #555; }
        .win { color: green; font-weight: bold; }
        .loss { color: red; }
    </style>
    """
    
    html = "<html><head>{}</head><body>".format(style)

    # 1. Current Season Performance
    season_total = season_wins + season_losses
    season_pct = (season_wins / season_total * 100) if season_total > 0 else 0.0

    html += "<h3>{} Season Performance</h3>".format(current_year)
    html += "<b>Record:</b> {}-{} ({:.1f}%)<br>".format(season_wins, season_losses, season_pct)
    html += "<b>Current Rank:</b> {} of {}<br>".format(rank, total_players_season)
    html += "<b>Tendencies:</b> {} Favorites / {} Underdogs<br><br>".format(season_fav, season_dog)
    
    html += "<table><tr><th>Week</th><th>Pick</th><th>Type</th><th>Result</th></tr>"
    for row in weekly_data:
        res_class = "win" if row['result'] == "Win" else ("loss" if (row['result'] == "Loss" or row['result'] == "Loss (Push)") else "")
        html += "<tr><td>{}</td><td>{}</td><td>{}</td><td class='{}'>{}</td></tr>".format(
            row['week'], row['pick'], row['type'], res_class, row['result']
        )
    html += "</table><br><br>"

    # 2. Team ATS Records
    html += "<h3>{} NFL Team ATS Records - LOTW Lines</h3>".format(current_year)
    html += "<table><tr><th>Team</th><th>Wins</th><th>Losses</th><th>Win %</th></tr>"
    for team in team_ats_records:
        html += "<tr><td>{}</td><td>{}</td><td>{}</td><td>{:.1f}%</td></tr>".format(
            team[0], team[1], team[2], team[3]*100
        )
    html += "</table><br><br>"

    # 3. Career Performance
    career_total = career_wins + career_losses
    career_pct = (career_wins / career_total * 100) if career_total > 0 else 0.0
    
    html += "<h3>Career Performance (since 2018)</h3>"
    html += "<b>Your Career Record:</b> {}-{} ({:.1f}%)<br>".format(career_wins, career_losses, career_pct)
    html += "<b>Your Career Tendencies:</b> {} Favorites / {} Underdogs<br><br>".format(career_fav, career_dog)

    # 4. All Players Career Standings
    html += "<h3>LOTW Career Standings (since 2018, minimum 2 seasons)</h3>"
    html += "<table><tr><th>Rank</th><th>Player</th><th>Wins</th><th>Losses</th><th>Win %</th></tr>"
    c_rank = 1
    for p in all_career_standings:
        # p structure: (id, name, wins, losses, pct)
        p_id_loop = p[0]
        p_name = p[1]
        
        # Bold name if it matches current player
        if p_id_loop == current_player_id:
            #p_name = "<b>{}</b>".format(p_name)
            html += "<tr><td><b>{}</b></td><td><b>{}</b></td><td><b>{}</b></td><td><b>{}</b></td><td><b>{:.1f}%</b></td></tr>".format(
                c_rank, p_name, p[2], p[3], p[4]*100
            )
        else:    
            html += "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{:.1f}%</td></tr>".format(
                c_rank, p_name, p[2], p[3], p[4]*100
            )
        c_rank += 1
    html += "</table><br><br>"

    html += "</body></html>"
    return html


# --- Main Lambda Handler ---

def lambda_handler(event, context):
    """
    Generate and email analytics reports.
    """
    logger.info("Received event: " + json.dumps(event, indent=2))
    
    request_type = event.get('detail-type', 'manual_run')
    
    # DB Connection
    db_endpoint = os.environ['db_endpoint']
    db_port = int(os.environ['db_port'])
    db_username = os.environ['db_username']
    db_password = os.environ['db_password']
    db_name = os.environ['db_name']

    try:
        conn = pymysql.connect(host=db_endpoint, port=db_port, user=db_username, passwd=db_password, db=db_name, connect_timeout=5)
    except Exception as e:
        logger.error("DB Connection failed: {}".format(str(e)))
        sys.exit()

    # Configuration
    mail_username = os.environ['mail_username']
    mail_password = os.environ['mail_password']
    mail_host = os.environ['mail_host']
    mail_port = os.environ['mail_port']
    mail_from = '"Brendan Connell" <bmoney312@lock-of-the-week.com>' # Or generic sender

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
    
    current_year = get_current_year()
    
    # Determine recipients
    #player_id_arg = os.environ.get('player_id')
    #if player_id_arg:
    #    players = get_player(conn, int(player_id_arg))
    #else:
    #    players = get_all_paid_players(conn)

    player_id = os.environ.get('player_id')
    players = []
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

    # --- Pre-calculate Global Stats (Shared across all emails) ---
    logger.info("Calculating Global Stats...")
    
    # 1. Team ATS Records for current year
    team_ats_records = get_team_ats_records(conn, current_year)
    
    # 2. All Players Career Standings (2018 to Current)
    all_career_standings = get_all_career_standings(conn, 2018, current_year)
    
    # 3. Current Year Standings (to get rank)
    current_standings = get_standings(conn) # List of tuples, need to parse to find rank
    total_players_season = len(current_standings)
    
    # Connect SMTP
    smtp_relay = smtp_connect(mail_host, mail_port, mail_username, mail_password)
    if not smtp_relay:
        conn.close()
        sys.exit()

    # --- Loop Players ---
    for player in players:
        p_id, p_email, last, first, _, _ = player
        logger.info("Generating report for {} {} ({})".format(first, last, p_id))
        
        # 1. Get Current Season Details
        weekly_data, s_fav, s_dog = get_player_season_details(conn, p_id, current_year)
        
        # Find Rank and Record from current standings
        s_wins = 0
        s_losses = 0
        rank = "-"
        
        # Standings tuple: (id, last, first, titles, rookie, wins, losses, win_pct, ats, streak)
        # We iterate to find the player and their index (rank)
        for i, row in enumerate(current_standings):
            if row[0] == p_id:
                rank = i + 1
                s_wins = row[5]
                s_losses = row[6]
                break
        
        # 2. Get Career Stats (Player specific)
        logger.info("Generating career stats for {} {} ({})".format(first, last, p_id))
        c_wins, c_losses, c_fav, c_dog = get_player_career_stats(conn, p_id, 2018, current_year)

        # 3. Build HTML
        html_body = build_analytics_html(
            first, current_year,
            weekly_data, s_fav, s_dog, s_wins, s_losses, rank, total_players_season,
            c_wins, c_losses, c_fav, c_dog,
            team_ats_records,
            all_career_standings,
            p_id
        )
        
        subject = "lotw: pick analytics report: {} {}".format(first, last)

        # 4. Send Email
        #logger.info("Sending report to {} {} ({})".format(first, last, p_id))
        #smtp_send(smtp_relay, subject, html_body, [p_email], mail_from)

        # 4. Send Email with Retry Logic
        logger.info("Sending report to {} {} ({})".format(first, last, p_id))
        email_sent_successfully = False
        for attempt in range(MAX_RETRIES):
            email_result = smtp_send(smtp_relay, subject, html_body, [p_email], mail_from)
            
            if email_result is True:
                logger.info("Email sent successfully to player {} {} on attempt {}".format(p_id, p_email, attempt + 1))
                email_sent_successfully = True
                break # Exit retry loop on success
            else:
                logger.error("Email failed to player {} {} on attempt {}".format(p_id, p_email, attempt + 1))
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
                    logger.error("All {} retry attempts failed for player {} {}".format(MAX_RETRIES, p_id, p_email))

        # If all retries failed, log and handle
        if not email_sent_successfully:
            logger.error("Aborting email send for player {} {} after all retries.".format(p_id, p_email))

            if smtp_relay is None:
                 logger.error("SMTP connection is dead.")

            # Close connections and exit with 504 error as requested
            conn.close()
            if smtp_relay:
                smtp_relay.close()
            return response(504, 'text/html', build_html("Analytics Report send failed for player {} after {} attempts. Aborting.".format(p_id, MAX_RETRIES)))

        # Gentle pacing
        sleep(1)

    smtp_relay.close()
    conn.close()
    return response(200, 'text/html', "Analytics reports sent successfully.")
