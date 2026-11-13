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
        conn.close()
        return response(400, 'text/html', build_html_response("Missing league_id or player_id"))

    with conn.cursor() as cur:
        # Validate player_id and retrieve player name
        cur.execute("SELECT first_name, last_name FROM Players WHERE player_id = %s", (player_id,))
        player_row = cur.fetchone()
        if not player_row:
            conn.close()
            return response(400, 'text/html', build_html_response("Invalid player_id"))
        player_name = "{} {}".format(player_row[0], player_row[1])

        # Validate league_id and retrieve league_name
        cur.execute("SELECT league_name FROM Leagues WHERE league_id = %s", (league_id,))
        league_row = cur.fetchone()
        if not league_row:
            conn.close()
            return response(400, 'text/html', build_html_response("Invalid league_id"))
        league_name = league_row[0]

    try:
        with conn.cursor() as cur:
            sql = "INSERT INTO League_Members (league_id, player_id) VALUES (%s, %s)"
            cur.execute(sql, (league_id, player_id))
            conn.commit()
            msg = "Successfully added player {} (ID: {}) to league {} (ID: {}).".format(
                player_name, player_id, league_name, league_id
            )
            logger.info(msg)
    except Exception as e:
        conn.close()
        # Handle duplicate entry error gracefully
        if 'Duplicate entry' in str(e):
            return response(
                200,
                'text/html',
                build_html_response(
                    "Player {} (ID: {}) is already in league {} (ID: {}).".format(
                        player_name, player_id, league_name, league_id
                    )
                ),
            )
        return response(500, 'text/html', build_html_response("Error adding player: {}".format(str(e))))

    conn.close()
    return response(200, 'text/html', build_html_response(msg))
