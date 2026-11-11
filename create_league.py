import json
import logging
import sys
from lotw import get_db_connection, validate_field, response, build_html_response

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    logger.info("Received event: " + json.dumps(event, indent=2))

    try:
        conn = get_db_connection()
    except Exception as e:
        logger.error("ERROR: Could not connect to MySQL database: {}".format(str(e)))
        sys.exit()

    query_string_params = event.get('queryStringParameters', {}) or event
    league_name = query_string_params.get('league_name')
    description = query_string_params.get('description', '')
    league_owner = query_string_params.get('league_owner')

    if not league_name:
        conn.close()
        return response(400, 'text/html', build_html_response("Missing league_name parameter"))

    if not league_owner:
        conn.close()
        return response(400, 'text/html', build_html_response("Missing mandatory league_owner (player_id) parameter"))

    try:
        league_owner = int(league_owner)
    except ValueError:
        conn.close()
        return response(400, 'text/html', build_html_response("Invalid league_owner: must be an integer player_id"))

    # Validate that the league_owner exists in the Players table
    if not validate_field(conn, str(league_owner), 'player_id', 'Players'):
        conn.close()
        return response(400, 'text/html', build_html_response(f"Invalid league_owner: player_id {league_owner} does not exist"))

    try:
        with conn.cursor() as cur:
            sql = "INSERT INTO Leagues (league_name, description, league_owner) VALUES (%s, %s, %s)"
            cur.execute(sql, (league_name, description, league_owner))
            conn.commit()
            league_id = cur.lastrowid
            msg = f"Successfully created league: {league_name} (ID: {league_id}, Owner: {league_owner})"
            logger.info(msg)
    except Exception as e:
        conn.close()
        return response(500, 'text/html', build_html_response("Error creating league: {}".format(str(e))))

    conn.close()
    return response(200, 'text/html', build_html_response(msg))
