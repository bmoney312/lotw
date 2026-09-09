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
    league_id = query_string_params.get('league_id')
    player_id = query_string_params.get('player_id')

    if not league_id or not player_id:
        return response(400, 'text/html', build_html_response("Missing league_id or player_id"))

    # Validate inputs
    if not validate_field(conn, league_id, 'league_id', 'Leagues'):
        return response(400, 'text/html', build_html_response("Invalid league_id"))
    if not validate_field(conn, player_id, 'player_id', 'Players'):
        return response(400, 'text/html', build_html_response("Invalid player_id"))

    try:
        with conn.cursor() as cur:
            sql = "INSERT INTO League_Members (league_id, player_id) VALUES (%s, %s)"
            cur.execute(sql, (league_id, player_id))
            conn.commit()
            msg = "Successfully added player {} to league {}.".format(player_id, league_id)
            logger.info(msg)
    except Exception as e:
        conn.close()
        # Handle duplicate entry error gracefully
        if 'Duplicate entry' in str(e):
             return response(200, 'text/html', build_html_response("Player is already in this league."))
        return response(500, 'text/html', build_html_response("Error adding player: {}".format(str(e))))

    conn.close()
    return response(200, 'text/html', build_html_response(msg))
