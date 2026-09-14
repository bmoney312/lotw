import os
import sys
import csv
import json
import logging
import urllib.request
from lotw import get_current_week, get_current_year, get_all_games, response, get_db_connection

# Global variables
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Map nflverse team abbreviations to LOTW database team IDs
# Key mapping: 'LA' -> 'LAR' (nflverse uses LA for Rams and LAC for Chargers)
NFLVERSE_TO_LOTW_TEAM_MAP = {
    'ARI': 'ARI', 'ATL': 'ATL', 'BAL': 'BAL', 'BUF': 'BUF',
    'CAR': 'CAR', 'CHI': 'CHI', 'CIN': 'CIN', 'CLE': 'CLE',
    'DAL': 'DAL', 'DEN': 'DEN', 'DET': 'DET', 'GB': 'GNB',
    'HOU': 'HOU', 'IND': 'IND', 'JAX': 'JAX', 'KC': 'KAN',
    'LAC': 'LAC', 'LAR': 'LAR', 'LA': 'LAR',
    'LV': 'LVR', 'LVR': 'LVR', 'OAK': 'LVR',
    'MIA': 'MIA', 'MIN': 'MIN', 'NO': 'NOR', 'NE': 'NWE',
    'NYG': 'NYG', 'NYJ': 'NYJ', 'PHI': 'PHI', 'PIT': 'PIT',
    'SEA': 'SEA', 'SF': 'SFO', 'TB': 'TAM', 'TEN': 'TEN',
    'WAS': 'WAS', 'WSH': 'WAS',
    # Direct database key passthroughs
    'GNB': 'GNB', 'KAN': 'KAN', 'NOR': 'NOR',
    'NWE': 'NWE', 'SFO': 'SFO', 'TAM': 'TAM'
}


def fetch_scores_from_nflverse(target_year, target_week):
    """
    Fetches game scores from the open nflverse GitHub raw CSV feed.
    Returns: { 'LOTW_TEAM_ID': score_int, ... }
    """
    url = "https://raw.githubusercontent.com/nflverse/nfldata/master/data/games.csv"
    logger.info("Fetching game results from: %s", url)

    req = urllib.request.Request(
        url,
        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    )

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            lines = resp.read().decode('utf-8').splitlines()
    except Exception as e:
        logger.error("Failed to download score data from nflverse: %s", str(e))
        return {}

    reader = csv.DictReader(lines)
    team_scores = {}

    for row in reader:
        # Match target season and week
        try:
            row_season = int(row.get('season', 0))
            row_week = int(row.get('week', 0))
        except (ValueError, TypeError):
            continue

        if row_season != int(target_year) or row_week != int(target_week):
            continue

        away_team_raw = row.get('away_team', '').strip().upper()
        home_team_raw = row.get('home_team', '').strip().upper()
        away_score_raw = row.get('away_score', '').strip()
        home_score_raw = row.get('home_score', '').strip()

        away_id = NFLVERSE_TO_LOTW_TEAM_MAP.get(away_team_raw, away_team_raw)
        home_id = NFLVERSE_TO_LOTW_TEAM_MAP.get(home_team_raw, home_team_raw)

        # Store integer scores if populated and numeric (completed/in-progress with score)
        if away_score_raw and away_score_raw.isdigit():
            team_scores[away_id] = int(away_score_raw)
        else:
            team_scores[away_id] = None

        if home_score_raw and home_score_raw.isdigit():
            team_scores[home_id] = int(home_score_raw)
        else:
            team_scores[home_id] = None

    logger.info("Parsed scores for %d teams in week %s", len(team_scores), target_week)
    return team_scores


def generate_sql_lines(conn, week):
    """
    Generate SQL UPDATE statements for the given week using nflverse scores.
    Returns plain newline-separated text.
    """
    year = get_current_year()
    table_name = f"Games_{year}"

    # Fetch scores from nflverse
    web_scores = fetch_scores_from_nflverse(year, week)

    # Fetch all games for the specific week from DB
    games = get_all_games(conn, week)

    if not games:
        logger.warning("No games found in DB for week %s", week)
        return ""

    sql_statements = []

    for game in games:
        # Schema of game tuple based on lotw.py:
        # (game_id, week, kickoff_time, away_team_id, home_team_id, home_team_line, ...)
        away_team_id = game[3]
        home_team_id = game[4]

        # Retrieve scores from parsed feed
        away_score = web_scores.get(away_team_id)
        home_score = web_scores.get(home_team_id)

        away_val_str = str(away_score) if away_score is not None else ""
        home_val_str = str(home_score) if home_score is not None else ""

        # Format matches expected SQL: UPDATE Games_YYYY SET away_team_score = X, home_team_score = Y ...
        sql_line = (
            f"UPDATE {table_name} "
            f"SET away_team_score = {away_val_str}, home_team_score = {home_val_str} "
            f"WHERE away_team_id = '{away_team_id}' AND home_team_id = '{home_team_id}' AND week = {week};"
        )
        sql_statements.append(sql_line)

    return "\n".join(sql_statements) + "\n"


def lambda_handler(event, context):
    """
    Generate a plain text SQL payload for updating game results.
    """
    logger.info("Received event: %s", json.dumps(event, indent=2))

    request_type = event.get('detail-type')
    if request_type is None:
        request_type = "manual_run"

    # Connect to lotw database
    try:
        conn = get_db_connection()
    except Exception as e:
        logger.error("ERROR: Could not connect to MySQL database: %s", str(e))
        sys.exit()

    logger.info("SUCCESS: Connection to MySQL database succeeded")

    # Determine target week
    week = os.environ.get('week')
    query_string_params = event.get('queryStringParameters')

    if query_string_params is not None and 'week' in query_string_params:
        week = query_string_params.get('week')

    if week is None:
        week = get_current_week(conn)
        if week is None:
            logger.error("ERROR: Unable to determine week!")
            conn.close()
            return response(400, 'text/plain', "Error: Unable to determine week.")
        # Default to preceding completed week when not explicitly specified
        week = int(week) - 1
    else:
        week = int(week)

    logger.info("Generating SQL for week %s", week)

    # Generate the SQL content
    try:
        sql_content = generate_sql_lines(conn, week)
    except Exception as e:
        logger.error("Error generating SQL: %s", str(e))
        conn.close()
        return response(500, 'text/plain', f"Error generating SQL: {str(e)}")

    # Close database connection
    conn.close()

    # Return as clean plain text
    return response(200, 'text/plain', sql_content)
