import sys
import json
import logging
from lotw import get_db_connection, response

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def format_ascii_table(headers, rows):
    """
    Renders an aligned ASCII table matching get_league_members output formatting.
    """
    if not rows:
        return "No leagues found."

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

    try:
        conn = get_db_connection()
    except Exception as e:
        logger.error("Database connection failure: %s", str(e))
        sys.exit()

    # Left join League_Members to calculate member counts for each league
    sql = """
        SELECT l.league_id, l.league_name, l.description, COUNT(lm.player_id) AS members
        FROM Leagues l
        LEFT JOIN League_Members lm ON l.league_id = lm.league_id
        GROUP BY l.league_id, l.league_name, l.description
        ORDER BY l.league_id ASC
    """

    with conn.cursor() as cur:
        cur.execute(sql)
        leagues = cur.fetchall()

    conn.close()

    headers = ["league_id", "league_name", "description", "members"]
    table_str = format_ascii_table(headers, leagues)

    output = (
        f"Total Leagues: {len(leagues)}\n\n"
        f"{table_str}"
    )

    print(output)
    return response(200, 'text/plain', output)
