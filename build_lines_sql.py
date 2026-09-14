import os
import sys
import json
import logging
import urllib.request
from lotw import get_db_connection, get_current_week, get_current_year, get_all_games, response

# Global variables
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# DraftKings NFL Event Group ID
DK_NFL_URL = "https://sportsbook.draftkings.com/sites/US-SB/api/v5/eventgroups/88808?format=json"

# Mapping DraftKings team name substrings/abbreviations to LOTW team IDs
DK_TEAM_TO_LOTW = {
    'cardinals': 'ARI', 'falcons': 'ATL', 'ravens': 'BAL', 'bills': 'BUF',
    'panthers': 'CAR', 'bears': 'CHI', 'bengals': 'CIN', 'browns': 'CLE',
    'cowboys': 'DAL', 'broncos': 'DEN', 'lions': 'DET', 'packers': 'GNB',
    'texans': 'HOU', 'colts': 'IND', 'jaguars': 'JAX', 'chiefs': 'KAN',
    'chargers': 'LAC', 'rams': 'LAR', 'raiders': 'LVR', 'dolphins': 'MIA',
    'vikings': 'MIN', 'saints': 'NOR', 'patriots': 'NWE', 'giants': 'NYG',
    'jets': 'NYJ', 'eagles': 'PHI', 'steelers': 'PIT', 'seahawks': 'SEA',
    '49ers': 'SFO', 'buccaneers': 'TAM', 'titans': 'TEN', 'commanders': 'WAS'
}


def normalize_line_to_integer(raw_line):
    """
    Rounds lines according to league rules:
    - If magnitude is between 6.5 and 7.5 inclusive, round to 7 (maintaining sign).
    - Otherwise, strip the half point (.5) to make it an integer.
    """
    if raw_line is None:
        return None

    line = float(raw_line)
    sign = -1 if line < 0 else 1
    abs_line = abs(line)

    # Always snap 6.5, 7.0, 7.5 to 7
    if 6.5 <= abs_line <= 7.5:
        return sign * 7

    # Otherwise strip the .5 / truncate towards zero
    return sign * int(abs_line)


def fetch_draftkings_spreads():
    """
    Queries DraftKings Sportsbook API for NFL game lines.
    Returns a dictionary of {(away_team_id, home_team_id): home_team_line_int}
    """
    logger.info("Fetching odds from DraftKings API: %s", DK_NFL_URL)
    req = urllib.request.Request(
        DK_NFL_URL,
        headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json'
        }
    )

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode('utf-8'))
    except Exception as e:
        logger.error("Failed to fetch odds from DraftKings: %s", str(e))
        return {}

    events = {ev['eventId']: ev for ev in data.get('events', [])}
    odds_map = {}

    # Category 488 / subcategory 4511 or standard game lines market
    for category in data.get('eventGroup', {}).get('offerCategories', []):
        for subcat in category.get('offerSubcategoryDescriptors', []):
            offer_table = subcat.get('offerSubcategory', {}).get('offers', [])
            for offer_row in offer_table:
                for offer in offer_row:
                    # Look for point spread market
                    if offer.get('label') != "Spread":
                        continue

                    event_id = offer.get('eventId')
                    event = events.get(event_id)
                    if not event:
                        continue

                    team1_name = event.get('teamName1', '').lower()
                    team2_name = event.get('teamName2', '').lower()

                    # Resolve away and home team IDs
                    team1_id = next((v for k, v in DK_TEAM_TO_LOTW.items() if k in team1_name), None)
                    team2_id = next((v for k, v in DK_TEAM_TO_LOTW.items() if k in team2_name), None)

                    if not team1_id or not team2_id:
                        continue

                    # DraftKings outcomes: match home team spread
                    for outcome in offer.get('outcomes', []):
                        label = outcome.get('label', '').lower()
                        # Team2 is conventionally the home team in DraftKings event definitions
                        if any(k in label for k in DK_TEAM_TO_LOTW if DK_TEAM_TO_LOTW[k] == team2_id):
                            raw_spread = outcome.get('line')
                            if raw_spread is not None:
                                int_line = normalize_line_to_integer(raw_spread)
                                # Map key by (away_id, home_id) and (home_id,)
                                odds_map[(team1_id, team2_id)] = int_line
                                odds_map[team2_id] = int_line

    logger.info("Parsed %d spread lines from DraftKings", len(odds_map))
    return odds_map


def generate_sql_lines(conn, week):
    """
    Generate SQL UPDATE statements for the given week with DraftKings lines filled in.
    """
    year = get_current_year()
    table_name = f"Games_{year}"

    # Fetch DraftKings spreads
    dk_spreads = fetch_draftkings_spreads()

    # Fetch all games for the specific week from DB
    games = get_all_games(conn, week)

    if not games:
        logger.warning("No games found for week %s", week)
        return ""

    sql_output = ""

    for game in games:
        game_id = game[0]
        away_team_id = game[3]
        home_team_id = game[4]

        # Look up line matching (away, home) pair or by home team ID
        line = dk_spreads.get((away_team_id, home_team_id), dk_spreads.get(home_team_id))
        line_str = str(line) if line is not None else ""

        # Format matches expected SQL: UPDATE Games_YYYY SET home_team_line = -3 WHERE game_id = 123 AND home_team_id = 'DEN';
        sql_line = (
            f"UPDATE {table_name} SET home_team_line = {line_str} "
            f"WHERE game_id = {game_id} AND home_team_id = '{home_team_id}';"
        )
        sql_output += sql_line + "<br>"

    return sql_output


def lambda_handler(event, context):
    """
    Generate a SQL file for updating game lines automatically from DraftKings.
    """
    logger.info("Received event: %s", json.dumps(event, indent=2))

    request_type = event.get('detail-type')
    if request_type is None:
        request_type = event.get('requestContext', {}).get('stage', 'manual_run')

    # Create lotw database connection
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
