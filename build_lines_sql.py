import os
import sys
import csv
import json
import logging
import urllib.request
from lotw import get_db_connection, get_current_week, get_current_year, get_all_games, response

# Global variables
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Map nflverse team abbreviations to LOTW database team IDs
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


def normalize_line_to_integer(raw_home_line):
    """
    Rounds home team spread according to LOTW league rules:
    - If magnitude is between 6.5 and 7.5 inclusive, round to 7 (preserving sign).
    - Otherwise, truncate the half-point (.5) to convert to an integer.
    """
    if raw_home_line is None or str(raw_home_line).strip() == "":
        return None

    try:
        line = float(raw_home_line)
    except (ValueError, TypeError):
        return None

    if line == 0:
        return 0

    sign = -1 if line < 0 else 1
    abs_line = abs(line)

    # Round 6.5, 7.0, 7.5 to 7
    if 6.5 <= abs_line <= 7.5:
        return sign * 7

    # Round 2.5 to 3.0
    if abs_line == 2.5:
        return sign * 3

    # Otherwise strip the .5 / truncate towards zero
    return sign * int(abs_line)


def fetch_nflverse_spreads(target_year, target_week):
    """
    Fetches game spread lines from the open nflverse CSV feed.
    Returns a dictionary: { (away_team_id, home_team_id): home_team_line_int, home_team_id: home_team_line_int }
    """
    url = "https://raw.githubusercontent.com/nflverse/nfldata/master/data/games.csv"
    logger.info("Fetching lines from nflverse: %s", url)

    req = urllib.request.Request(
        url,
        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    )

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            lines = resp.read().decode('utf-8').splitlines()
    except Exception as e:
        logger.error("Failed to download lines from nflverse: %s", str(e))
        return {}

    reader = csv.DictReader(lines)
    spreads_map = {}

    for row in reader:
        try:
            row_season = int(row.get('season', 0))
            row_week = int(row.get('week', 0))
        except (ValueError, TypeError):
            continue

        if row_season != int(target_year) or row_week != int(target_week):
            continue

        away_raw = row.get('away_team', '').strip().upper()
        home_raw = row.get('home_team', '').strip().upper()
        raw_spread = row.get('spread_line', '').strip()

        away_id = NFLVERSE_TO_LOTW_TEAM_MAP.get(away_raw, away_raw)
        home_id = NFLVERSE_TO_LOTW_TEAM_MAP.get(home_raw, home_raw)

        if raw_spread:
            # In nflverse: positive spread_line means home favored by X -> home_line = -X
            # negative spread_line means away favored by X -> home_line = +X
            try:
                home_team_spread = -float(raw_spread)
                int_line = normalize_line_to_integer(home_team_spread)
                spreads_map[(away_id, home_id)] = int_line
                spreads_map[home_id] = int_line
            except ValueError:
                continue

    logger.info("Parsed %d game spreads for week %s", len(spreads_map) // 2, target_week)
    return spreads_map


def generate_sql_lines(conn, week):
    """
    Generate SQL UPDATE statements for the given week with spreads filled in.
    """
    year = get_current_year()
    table_name = f"Games_{year}"

    spreads_map = fetch_nflverse_spreads(year, week)
    games = get_all_games(conn, week)

    if not games:
        logger.warning("No games found for week %s", week)
        return ""

    sql_output = ""

    for game in games:
        game_id = game[0]
        away_team_id = game[3]
        home_team_id = game[4]

        # Match by (away, home) pair or by home team ID
        line = spreads_map.get((away_team_id, home_team_id), spreads_map.get(home_team_id))
        line_str = str(line) if line is not None else ""

        # Format: UPDATE Games_YYYY SET home_team_line = -3 WHERE game_id = 123 AND away_team_id = 'KC' AND home_team_id = 'DEN';
        sql_line = (
            f"UPDATE {table_name} SET home_team_line = {line_str} "
            f"WHERE game_id = {game_id} AND away_team_id = '{away_team_id}' AND home_team_id = '{home_team_id}';"
        )
        sql_output += sql_line + "<br>"

    return sql_output


def lambda_handler(event, context):
    """
    Generate a SQL file for updating game lines.
    """
    logger.info("Received event: %s", json.dumps(event, indent=2))

    request_type = event.get('detail-type')
    if request_type is None:
        request_type = event.get('requestContext', {}).get('stage', 'manual_run')

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
        logger.info("No 'week' in env or params, attempting to get current week.")
        week = get_current_week(conn)
    else:
        week = int(week)

    if week is None:
        logger.error("ERROR: Unable to determine week!")
        conn.close()
        return response(400, 'text/plain', "Error: Unable to determine week.")

    logger.info("Generating SQL for week %s", week)

    try:
        sql_content = generate_sql_lines(conn, week)
        if sql_content is None:
            raise Exception("Failed to generate SQL lines.")
    except Exception as e:
        logger.error("Error generating SQL: %s", str(e))
        conn.close()
        return response(500, 'text/plain', f"Error generating SQL: {str(e)}")

    conn.close()

    sql_html = f"<html><body>{sql_content}</body></html>"
    return response(200, 'text/html', sql_html)
