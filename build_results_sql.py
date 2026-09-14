import os
import sys
import json
import logging
import urllib.request
from lotw import get_current_week, get_current_year, get_all_games, response, get_db_connection

# Global logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Map ESPN abbreviations to LOTW database team_id abbreviations
ESPN_TO_LOTW_TEAM_MAP = {
    'ARI': 'ARI', 'ATL': 'ATL', 'BAL': 'BAL', 'BUF': 'BUF',
    'CAR': 'CAR', 'CHI': 'CHI', 'CIN': 'CIN', 'CLE': 'CLE',
    'DAL': 'DAL', 'DEN': 'DEN', 'DET': 'DET', 'GB': 'GNB',
    'HOU': 'HOU', 'IND': 'IND', 'JAX': 'JAX', 'KC': 'KAN',
    'LAC': 'LAC', 'LAR': 'LAR', 'LV': 'LVR', 'MIA': 'MIA',
    'MIN': 'MIN', 'NO': 'NOR', 'NE': 'NWE', 'NYG': 'NYG',
    'NYJ': 'NYJ', 'PHI': 'PHI', 'PIT': 'PIT', 'SEA': 'SEA',
    'SF': 'SFO', 'TB': 'TAM', 'TEN': 'TEN', 'WSH': 'WAS',
    # Fallbacks in case ESPN returns alternative keys
    'WAS': 'WAS', 'GNB': 'GNB', 'KAN': 'KAN', 'LVR': 'LVR',
    'NOR': 'NOR', 'NWE': 'NWE', 'SFO': 'SFO', 'TAM': 'TAM'
}


def fetch_espn_scores(year, week):
    """
    Fetches completed or live scores from ESPN's open JSON API.
    Returns: { 'LOTW_TEAM_ID': score_int, ... }
    """
    # Weeks 1-18: Regular Season (seasontype=2)
    # Weeks 19-22: Postseason (seasontype=3, weeks 1 to 4)
    if week > 18:
        season_type = 3
        api_week = week - 18
    else:
        season_type = 2
        api_week = week

    url = (
        f"https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"
        f"?dates={year}&week={api_week}&seasontype={season_type}"
    )
    logger.info("Fetching scores from ESPN: %s", url)

    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (LockOfTheWeek/2.0)"}
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode('utf-8'))
    except Exception as e:
        logger.error("Failed to query ESPN API: %s", str(e))
        return {}

    team_scores = {}
    events = data.get('events', [])

    for event in events:
        competitions = event.get('competitions', [])
        if not competitions:
            continue

        competitors = competitions[0].get('competitors', [])
        for team in competitors:
            raw_abbr = team.get('team', {}).get('abbreviation', '').upper()
            score_val = team.get('score')

            lotw_team_id = ESPN_TO_LOTW_TEAM_MAP.get(raw_abbr, raw_abbr)

            if score_val is not None and str(score_val).isdigit():
                team_scores[lotw_team_id] = int(score_val)
            else:
                team_scores[lotw_team_id] = None

    logger.info("Successfully parsed ESPN scores for: %s", list(team_scores.keys()))
    return team_scores


def generate_sql_lines(conn, week):
    """
    Generate SQL UPDATE statements for the given week using ESPN scores.
    """
    year = get_current_year()
    table_name = f"Games_{year}"

    web_scores = fetch_espn_scores(year, week)
    games = get_all_games(conn, week)

    if not games:
        logger.warning("No games found in DB for week %s", week)
        return ""

    sql_output = ""

    for game in games:
        # Tuple schema: (game_id, week, kickoff_time, away_team_id, home_team_id, ...)
        away_team_id = game[3]
        home_team_id = game[4]

        away_score = web_scores.get(away_team_id)
        home_score = web_scores.get(home_team_id)

        away_val_str = str(away_score) if away_score is not None else ""
        home_val_str = str(home_score) if home_score is not None else ""

        sql_line = (
            f"UPDATE {table_name} "
            f"SET away_team_score = {away_val_str}, home_team_score = {home_val_str} "
            f"WHERE away_team_id = '{away_team_id}' AND home_team_id = '{home_team_id}' AND week = {week};"
        )
        sql_output += sql_line + "<br>"

    return sql_output


def lambda_handler(event, context):
    """
    Generate a SQL file for updating game results using the ESPN JSON API.
    """
    logger.info("Received event: %s", json.dumps(event, indent=2))

    try:
        conn = get_db_connection()
    except Exception as e:
        logger.error("ERROR: Could not connect to MySQL database: %s", str(e))
        sys.exit()

    logger.info("SUCCESS: Connection to MySQL database succeeded")

    # Determine target week
    week = os.environ.get('week')
    query_string_params = event.get('queryStringParameters')

    if query_string_params and 'week' in query_string_params:
        week = query_string_params.get('week')

    if week is None:
        current_week = get_current_week(conn)
        if current_week is None:
            logger.error("ERROR: Unable to determine week!")
            conn.close()
            return response(400, 'text/plain', "Error: Unable to determine week.")
        # Default to preceding completed week when not specified
        week = current_week - 1
    else:
        week = int(week)

    logger.info("Generating SQL for week %s", week)

    try:
        sql_content = generate_sql_lines(conn, week)
    except Exception as e:
        logger.error("Error generating SQL: %s", str(e))
        conn.close()
        return response(500, 'text/plain', f"Error generating SQL: {str(e)}")

    conn.close()

    sql_html = f"<html><body>{sql_content}</body></html>"
    return response(200, 'text/html', sql_html)
