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
    league_name = query_string_params.get('league_name')
    description = query_string_params.get('description', '')

    if not league_name:
        return response(400, 'text/html', build_html_response("Missing league_name parameter"))

    try:
        with conn.cursor() as cur:
            sql = "INSERT INTO Leagues (league_name, description) VALUES (%s, %s)"
            cur.execute(sql, (league_name, description))
            conn.commit()
            league_id = cur.lastrowid
            msg = "Successfully created league: {} (ID: {})".format(league_name, league_id)
            logger.info(msg)
    except Exception as e:
        conn.close()
        return response(500, 'text/html', build_html_response("Error creating league: {}".format(str(e))))

    conn.close()
    return response(200, 'text/html', build_html_response(msg))
