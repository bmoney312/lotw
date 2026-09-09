import json
import logging
import sys
from lotw import get_db_connection, response, build_html_response

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

    if int(league_id) == 1:
        return response(403, 'text/html', build_html_response("Players cannot be removed from the Main Event league."))

    try:
        with conn.cursor() as cur:
            sql = "DELETE FROM League_Members WHERE league_id = %s AND player_id = %s"
            cur.execute(sql, (league_id, player_id))
            if cur.rowcount == 0:
                msg = "Player {} was not found in league {}.".format(player_id, league_id)
            else:
                msg = "Successfully removed player {} from league {}.".format(player_id, league_id)
            conn.commit()
            logger.info(msg)
    except Exception as e:
        conn.close()
        return response(500, 'text/html', build_html_response("Error removing player: {}".format(str(e))))

    conn.close()
    return response(200, 'text/html', build_html_response(msg))
