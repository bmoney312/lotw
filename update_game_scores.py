import os
import sys
import csv
import json
import logging
import urllib.request
from lotw import get_current_week, get_current_year, get_all_games, response, get_db_connection

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

        if away_score_raw and away_score_raw.isdigit():
            team_scores[away_id] = int(away_score_raw)
        else:
            team_scores[away_id] = None

        if home_score_raw and home_score_raw.isdigit():
            team_scores[home_id] = int(home_score_raw)
        else:
            team_scores[home_id] = None

    logger.info("Parsed %d team scores from nflverse for week %s", len(team_scores), target_week)
    return team_scores


def update_database_scores(conn, current_year, week, commit_flag):
    """
    Finds games for current_year and week, then updates scores in Games_{year}.
    If commit_flag is False, runs as dry-run and skips conn.commit().
    """
    table_name = f"Games_{current_year}"
    web_scores = fetch_scores_from_nflverse(current_year, week)
    games = get_all_games(conn, week)

    if not games:
        logger.warning("No games found in DB table %s for week %s", table_name, week)
        return {"updated": 0, "skipped": 0, "details": []}

    updated_count = 0
    skipped_count = 0
    run_details = []

    with conn.cursor() as cur:
        for game in games:
            game_id = game[0]
            away_team_id = game[3]
            home_team_id = game[4]
            current_away_score = game[6]
            current_home_score = game[7]

            new_away_score = web_scores.get(away_team_id)
            new_home_score = web_scores.get(home_team_id)

            # Skip games that have no score available yet (unplayed or in-progress without points)
            if new_away_score is None or new_home_score is None:
                logger.info(
                    "Skipping %s @ %s (Week %s): score not finalized yet in nflverse feed",
                    away_team_id, home_team_id, week
                )
                skipped_count += 1
                continue

            # Check if scores are already up to date
            if current_away_score == new_away_score and current_home_score == new_home_score:
                logger.info(
                    "Unchanged: %s (%s) @ %s (%s) already matches DB",
                    away_team_id, new_away_score, home_team_id, new_home_score
                )
                skipped_count += 1
                continue

            action_desc = (
                f"{away_team_id} {new_away_score} @ {home_team_id} {new_home_score} "
                f"(Prev: {current_away_score}-{current_home_score})"
            )

            if commit_flag:
                sql = (
                    f"UPDATE `{table_name}` "
                    f"SET `away_team_score` = %s, `home_team_score` = %s "
                    f"WHERE `game_id` = %s AND `week` = %s"
                )
                cur.execute(sql, (new_away_score, new_home_score, game_id, week))
                logger.info("COMMITTED: Updated Game %s -> %s", game_id, action_desc)
            else:
                logger.info("DRY-RUN: Would update Game %s -> %s", game_id, action_desc)

            updated_count += 1
            run_details.append(action_desc)

        if commit_flag:
            conn.commit()
            logger.info("Database commit successful for %d updated games.", updated_count)
        else:
            logger.info("Dry run complete: %d games would be updated (no changes committed).", updated_count)

    return {"updated": updated_count, "skipped": skipped_count, "details": run_details}


def lambda_handler(event, context):
    logger.info("Received event: %s", json.dumps(event, indent=2))

    request_type = event.get('detail-type', 'manual_run')
    mode = os.environ.get('mode', 'dry_run').strip().lower()

    # Allow query parameter override for manual testing if needed
    query_params = event.get('queryStringParameters') or {}
    if 'mode' in query_params:
        mode = query_params.get('mode').strip().lower()

    # Commit only if explicitly configured AND not running as a test event
    commit_flag = (mode == "commit" and request_type != "test")
    logger.info("Execution mode: '%s' | Request type: '%s' | Commit enabled: %s", mode, request_type, commit_flag)

    try:
        conn = get_db_connection()
    except Exception as e:
        logger.error("ERROR: Could not connect to MySQL database: %s", str(e))
        sys.exit()

    logger.info("SUCCESS: Connection to MySQL database succeeded")

    # Enforce current year restriction
    current_year = get_current_year()
    logger.info("Target season locked to current year: %s", current_year)

    # Determine single week (defaulting to current_week)
    target_week = os.environ.get('week')
    if query_params.get('week'):
        target_week = query_params.get('week')

    if target_week is None:
        target_week = get_current_week(conn)
        logger.info("Defaulting to current week: %s", target_week)
    else:
        target_week = int(target_week)

    if target_week is None:
        logger.error("ERROR: Unable to determine week!")
        conn.close()
        return response(400, 'text/plain', "Error: Unable to determine week.")

    logger.info("Processing game scores for Year %s, Week %s", current_year, target_week)

    try:
        summary = update_database_scores(conn, current_year, target_week, commit_flag)
    except Exception as e:
        logger.error("Error updating game scores: %s", str(e))
        conn.close()
        return response(500, 'text/plain', f"Error updating game scores: {str(e)}")

    conn.close()

    status_str = "COMMITTED" if commit_flag else "DRY-RUN (Simulated)"
    result_text = (
        f"Status: {status_str}\n"
        f"Season: {current_year} | Week: {target_week}\n"
        f"Games Updated: {summary['updated']} | Games Skipped/Unchanged: {summary['skipped']}\n\n"
        f"Details:\n" + ("\n".join(f"- {d}" for d in summary['details']) if summary['details'] else "None")
    )

    return response(200, 'text/plain', result_text)
