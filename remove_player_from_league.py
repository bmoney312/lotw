import json
import logging
import sys
from lotw import get_db_connection, get_player, response, build_html_response

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

    if int(league_id) == 1:
        conn.close()
        return response(403, 'text/html', build_html_response("Players cannot be removed from the Main Event league."))

    # Fetch player info using lotw.get_player
    # Note: lotw.get_player_info has a bug where it selects first_name twice instead of last_name
    player_rows = get_player(conn, player_id)
    if not player_rows:
        conn.close()
        return response(400, 'text/html', build_html_response("Invalid player_id"))
    _, _, last_name, first_name, _, _ = player_rows[0]
    player_name = "{} {}".format(first_name, last_name)

    # Fetch league_name
    with conn.cursor() as cur:
        cur.execute("SELECT league_name FROM Leagues WHERE league_id = %s", (league_id,))
        league_row = cur.fetchone()
        if not league_row:
            conn.close()
            return response(400, 'text/html', build_html_response("Invalid league_id"))
        league_name = league_row[0]

    try:
        with conn.cursor() as cur:
            sql = "DELETE FROM League_Members WHERE league_id = %s AND player_id = %s"
            cur.execute(sql, (league_id, player_id))
            if cur.rowcount == 0:
                msg = "Player {} (ID: {}) was not found in league {} (ID: {}).".format(
                    player_name, player_id, league_name, league_id
                )
            else:
                msg = "Successfully removed player {} (ID: {}) from league {} (ID: {}).".format(
                    player_name, player_id, league_name, league_id
                )
            conn.commit()
            logger.info(msg)
    except Exception as e:
        conn.close()
        return response(500, 'text/html', build_html_response("Error removing player: {}".format(str(e))))

    conn.close()
    return response(200, 'text/html', build_html_response(msg))
