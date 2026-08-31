import os
import sys
import json
import pymysql
import logging
import urllib.request
from html.parser import HTMLParser
from lotw import get_current_week, get_current_year, get_all_games, response

# global variables
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Team ID to Nickname mapping based on user provided table
# Note: 'Raiders' appears twice (LVR, OAK). Defaulting 'Raiders' to 'LVR' for 2025.
TEAM_NICKNAMES_MAP = {
    'ARI': 'Cardinals', 'ATL': 'Falcons', 'BAL': 'Ravens', 'BUF': 'Bills',
    'CAR': 'Panthers', 'CHI': 'Bears', 'CIN': 'Bengals', 'CLE': 'Browns',
    'DAL': 'Cowboys', 'DEN': 'Broncos', 'DET': 'Lions', 'GNB': 'Packers',
    'HOU': 'Texans', 'IND': 'Colts', 'JAX': 'Jaguars', 'KAN': 'Chiefs',
    'LAC': 'Chargers', 'LAR': 'Rams', 'LVR': 'Raiders', 'MIA': 'Dolphins',
    'MIN': 'Vikings', 'NOR': 'Saints', 'NWE': 'Patriots', 'NYG': 'Giants',
    'NYJ': 'Jets', 'PHI': 'Eagles', 'PIT': 'Steelers', 'SEA': 'Seahawks',
    'SFO': '49ers', 'TAM': 'Buccaneers', 'TEN': 'Titans', 'WAS': 'Commanders'
}

# Reverse mapping to find ID from Nickname string found on web
# We normalize to lowercase for easier matching
NICKNAME_TO_ID = {v.lower(): k for k, v in TEAM_NICKNAMES_MAP.items()}
# Add abbreviations (lowercased) to the map to support 'ari', 'dal', etc.
for k in TEAM_NICKNAMES_MAP.keys():
    NICKNAME_TO_ID[k.lower()] = k
# Handle old Oakland code if necessary, ensuring Raiders maps to LVR for 2025
NICKNAME_TO_ID['raiders'] = 'LVR'


class NFLScoreParser(HTMLParser):
    """
    Simple HTML Parser to extract Team Names and integers that look like scores.
    It produces a flat list of found teams and adjacent numbers.
    """
    def __init__(self):
        super().__init__()
        self.found_items = []  # List of {'type': 'team'|'score', 'value': ...}
        self.current_data = []

    def handle_data(self, data):
        clean_data = data.strip()
        if not clean_data:
            return

        lower_data = clean_data.lower()
        found_team = False

        # 1. Check for Exact Match (Fastest)
        if lower_data in NICKNAME_TO_ID:
            self.found_items.append({'type': 'team', 'value': NICKNAME_TO_ID[lower_data]})
            found_team = True
        else:
            # 2. Check for Substring Match (e.g. "Dallas Cowboys" contains "cowboys")
            for nickname, tid in NICKNAME_TO_ID.items():
                if nickname in lower_data:
                    self.found_items.append({'type': 'team', 'value': tid})
                    found_team = True
                    break

        if found_team:
            return

        # Check if data is a score (integer)
        # We assume scores are typically between 0 and 99
        if clean_data.isdigit():
            val = int(clean_data)
            if 0 <= val < 100:
                self.found_items.append({'type': 'score', 'value': val})


def fetch_web_scores(week):
    """
    Fetches HTML from NFL.com and attempts to parse scores.
    Returns a dictionary: { 'TEAM_ID': score, ... }
    """
    url = "https://www.nfl.com/schedules/2025/by-week/reg-{}".format(week)
    logger.info("Fetching scores from: {}".format(url))

    try:
        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        )
        with urllib.request.urlopen(req) as response:
            html_content = response.read().decode('utf-8')
    except Exception as e:
        logger.error("Failed to fetch web page: {}".format(str(e)))
        return {}

    parser = NFLScoreParser()
    parser.feed(html_content)

    # Match Team IDs to the nearest Score found
    team_scores = {}
    items = parser.found_items

    # NEW LOGIC: Iterate and match a team ID to the nearest available score,
    # skipping intermediary items (like 'FINAL', 'W', etc.)
    i = 0
    while i < len(items):
        current = items[i]

        if current['type'] == 'team':
            team_id = current['value']
            logger.debug(f"Items: {items}")

            # Look forward up to 5 positions (arbitrary, but generous limit) for the nearest score
            found_score = False
            for j in range(i + 1, min(i + 6, len(items))):
                if items[j]['type'] == 'score':
                    score = items[j]['value']
                    team_scores[team_id] = score
                    logger.debug(f"Matched Team: {team_id} with Score: {score}")

                    # Advance index past the team (i) AND the score (j)
                    # We continue from the item *after* the score
                    i = j + 1
                    found_score = True
                    break

            if not found_score:
                # If no score found near this team, just move to the next item
                i += 1
                logger.debug(f"No score found near team: {team_id}")
        else:
            # If not a team, just move to the next item
            i += 1

    logger.info("Parsed scores for teams: {}".format(list(team_scores.keys())))
    return team_scores


def generate_sql_lines(conn, week):
    """
    Generate SQL UPDATE statements for the given week using web scores.
    """
    year = get_current_year()
    table_name = "Games_" + str(year)

    # Fetch actual scores from the web
    web_scores = fetch_web_scores(week)

    # Fetch all games for the specific week from DB
    games = get_all_games(conn, week)

    sql_output = ""

    for game in games:
        # Schema of game tuple based on lotw.py:
        # (game_id, week, kickoff_time, away_team_id, home_team_id, home_team_line, ...)
        away_team_id = game[3]
        home_team_id = game[4]

        # Retrieve scores from our web fetch, default to empty string if not found
        away_score = web_scores.get(away_team_id, "")
        home_score = web_scores.get(home_team_id, "")

        # Only generate SQL if we actually found scores (optional, currently generates empty vals if missing)
        # Format: UPDATE Games_2025 SET away_team_score = 7, home_team_score = 10 ...
        sql_line = "UPDATE {} SET away_team_score = {}, home_team_score = {} WHERE away_team_id = '{}' AND home_team_id = '{}' AND week = {};".format(
            table_name,
            away_score if away_score != "" else "NULL",
            home_score if home_score != "" else "NULL",
            away_team_id,
            home_team_id,
            week
        )

        # Clean up "NULL" to empty space if you strictly want the example format "score = ,",
        # but standard SQL requires a value or NULL.
        # To match your exact requested format "score = ,":
        if away_score == "":
            sql_line = sql_line.replace("away_team_score = NULL", "away_team_score = ")
        if home_score == "":
            sql_line = sql_line.replace("home_team_score = NULL", "home_team_score = ")

        sql_output += sql_line + "<br>"

    return sql_output


def lambda_handler(event, context):
    """
    Generate a SQL file for updating game results with web scraping
    """
    logger.info("Received event: " + json.dumps(event, indent=2))

    request_type = event.get('detail-type')
    if request_type is None:
        request_type = "manual_run"

    db_endpoint = os.environ['db_endpoint']
    db_port = int(os.environ['db_port'])
    db_username = os.environ['db_username']
    db_password = os.environ['db_password']
    db_name = os.environ['db_name']

    logger.info("Connecting to MySQL database {}".format(db_endpoint))

    try:
        conn = pymysql.connect(host=db_endpoint, port=db_port,
                               user=db_username, passwd=db_password,
                               db=db_name, connect_timeout=5)
    except Exception as e:
        logger.error("ERROR: Unexpected error: Could not connect to MySQL database - {}".format(str(e)))
        sys.exit()

    logger.info("SUCCESS: Connection to MySQL database succeeded")

    # Determine target week
    week = os.environ.get('week')
    query_string_params = event.get('queryStringParameters')

    if query_string_params is not None and 'week' in query_string_params:
        week = query_string_params.get('week')

    if week is None:
        week = get_current_week(conn)
    else:
        week = int(week)

    if week is None:
        logger.error("ERROR: Unable to determine week!")
        conn.close()
        sys.exit()
    else:
        week = week - 1

    logger.info("Generating SQL for week {}".format(week))

    # Generate the content
    try:
        sql_content = generate_sql_lines(conn, week)
    except Exception as e:
        logger.error("Error generating SQL: {}".format(str(e)))
        conn.close()
        return response(500, 'text/plain', "Error generating SQL: " + str(e))

    # Close database connection
    conn.close()

    sql_html = "<html><body>" + sql_content + "</body></html>"
    return response(200, 'text/html', sql_html)
