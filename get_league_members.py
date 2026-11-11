import sys
import json
import logging
from lotw import get_db_connection, response

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def format_ascii_table(headers, rows):
    """
    Renders an aligned ASCII table matching update_player output formatting.
    """
    if not rows:
        return "No players found in this league."

    str_rows = [[str(val if val is not None else "NULL") for val in row] for row in rows]
    col_widths = [
        max(len(h), max((len(r[i]) for r in str_rows), default=0))
        for i, h in enumerate(headers)
    ]

    header_line = " | ".join(f"{h:<{col_widths[i]}}" for i, h in enumerate(headers))
    divider_line = "-+-".join("-" * col_widths[i] for i in range(len(headers)))
    data_lines = [
        " | ".join(f"{r[i]:<{col_widths[i]}}" for i in range(len(headers)))
        for r in str_rows
    ]

    return "\n".join([header_line, divider_line] + data_lines)


def lambda_handler(event, context):
    logger.info("Received event: %s", json.dumps(event, indent=2))

    query_params = event.get('queryStringParameters') or event or {}
    league_param = query_params.get('league') or query_params.get('league_id')

    if not league_param:
        return response(400, 'text/plain', "Error: Missing required parameter 'league' (id or name).")

    try:
        conn = get_db_connection()
    except Exception as e:
        logger.error("Database connection failure: %s", str(e))
        sys.exit()

    league_id = None
    league_name = None
    description = ""

    with conn.cursor() as cur:
        # Match by numeric ID or by league name (case-insensitive)
        if str(league_param).isdigit():
            cur.execute("SELECT league_id, league_name, description FROM Leagues WHERE league_id = %s", (int(league_param),))
        else:
            cur.execute("SELECT league_id, league_name, description FROM Leagues WHERE LOWER(league_name) = LOWER(%s)", (str(league_param).strip(),))

        league_meta = cur.fetchone()
        if not league_meta:
            conn.close()
            return response(404, 'text/plain', f"Error: League '{league_param}' not found.")

        league_id, league_name, description = league_meta

        # Query member players in the league
        sql = """
            SELECT p.player_id, p.last_name, p.first_name, p.email, p.past_titles, p.rookie
            FROM League_Members lm
            INNER JOIN Players p ON lm.player_id = p.player_id
            WHERE lm.league_id = %s
            ORDER BY p.last_name ASC, p.first_name ASC
        """
        cur.execute(sql, (league_id,))
        players = cur.fetchall()

    conn.close()

    headers = ["player_id", "last_name", "first_name", "email", "titles", "rookie"]
    table_str = format_ascii_table(headers, players)

    output = (
        f"League: {league_name} (ID: {league_id})\n"
        f"Description: {description or 'None'}\n"
        f"Member Count: {len(players)}\n\n"
        f"{table_str}"
    )

    print(output)
    return response(200, 'text/plain', output)
