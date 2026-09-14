import os
import re
import sys
import json
import logging
from lotw import get_db_connection, response, validate_field

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def format_ascii_table(headers, rows):
    """
    Renders an aligned ASCII table for clean reading in CloudWatch logs or plain text responses.
    """
    if not rows:
        return "No records found."

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


def fetch_live_table_schema(conn):
    """
    Queries MySQL database to retrieve current column definitions from DESCRIBE Players.
    Returns: dict mapping col_name -> {'type_raw': str, 'nullable': bool, 'key': str}
    """
    schema = {}
    with conn.cursor() as cur:
        cur.execute("DESCRIBE `Players`")
        for row in cur.fetchall():
            # row schema: (Field, Type, Null, Key, Default, Extra)
            col_name = row[0]
            col_type = row[1].lower()
            is_nullable = (row[2].upper() == 'YES')
            col_key = row[3]

            schema[col_name] = {
                'type_raw': col_type,
                'nullable': is_nullable,
                'key': col_key
            }
    return schema


def validate_and_cast_dynamic(field_name, raw_value, schema):
    """
    Dynamically inspects live column metadata from the database:
    - Rejects player_id and primary key updates.
    - Validates nullability constraints (NOT NULL).
    - Dynamically parses varchar/char length limits from parentheses.
    - Dynamically checks numeric boundaries (tinyint(1) booleans vs unsigned ranges).
    """
    # 1. Block player_id explicitly
    if field_name.lower() == 'player_id':
        return False, None, "Updating 'player_id' is strictly prohibited."

    # 2. Check if field exists in the live schema
    matched_col = next((c for c in schema.keys() if c.lower() == field_name.lower()), None)
    if not matched_col:
        valid_cols = ", ".join([c for c in schema.keys() if c != 'player_id'])
        return False, None, f"Field '{field_name}' does not exist in Players table. Valid fields: {valid_cols}"

    col_meta = schema[matched_col]
    col_type = col_meta['type_raw']
    is_nullable = col_meta['nullable']

    # 3. Block any primary key
    if col_meta['key'] == 'PRI':
        return False, None, f"Field '{matched_col}' is a Primary Key and cannot be updated."

    # 4. Handle NULL assignments
    cleaned_val = raw_value.strip()
    if cleaned_val.lower() in ('null', 'none'):
        if not is_nullable:
            return False, None, f"Field '{matched_col}' does not permit NULL (defined as NOT NULL)."
        return True, None, "Valid NULL"

    # 5. Dynamic validation for string/character types
    if any(s in col_type for s in ('varchar', 'char', 'text')):
        match = re.search(r'\((\d+)\)', col_type)
        if match:
            max_len = int(match.group(1))
            if len(cleaned_val) > max_len:
                return False, None, f"Value exceeds maximum allowed length of {max_len} characters for '{matched_col}'."

        if len(cleaned_val) == 0 and not is_nullable:
            return False, None, f"Field '{matched_col}' is NOT NULL and cannot be empty."

        return True, cleaned_val, "Valid string"

    # 6. Dynamic validation for integer types
    elif 'int' in col_type:
        try:
            int_val = int(cleaned_val)
        except ValueError:
            return False, None, f"Field '{matched_col}' expects an integer; received '{raw_value}'."

        if col_type.startswith('tinyint(1)'):
            if int_val not in (0, 1):
                return False, None, f"Field '{matched_col}' is a boolean flag (tinyint(1)); must be 0 or 1."
            return True, int_val, "Valid boolean integer"

        elif 'tinyint' in col_type and 'unsigned' in col_type:
            if not (0 <= int_val <= 255):
                return False, None, f"Field '{matched_col}' is tinyint unsigned; value must be between 0 and 255."
            return True, int_val, "Valid tinyint unsigned"

        elif 'tinyint' in col_type:
            if not (-128 <= int_val <= 127):
                return False, None, f"Field '{matched_col}' is signed tinyint; value must be between -128 and 127."
            return True, int_val, "Valid signed tinyint"

        return True, int_val, "Valid integer"

    return True, cleaned_val, "Valid value"


def pretty_print_players(conn, player_id=None):
    """
    Retrieves and pretty-prints either a single player or the full Players table.
    """
    with conn.cursor() as cur:
        if player_id is not None:
            cur.execute("SELECT * FROM `Players` WHERE `player_id` = %s", (player_id,))
        else:
            cur.execute("SELECT * FROM `Players` ORDER BY `player_id` ASC")

        rows = cur.fetchall()
        headers = [desc[0] for desc in cur.description]

    return format_ascii_table(headers, rows)


def lambda_handler(event, context):
    logger.info("Received event: %s", json.dumps(event, indent=2))

    request_type = event.get('detail-type', 'manual_run')
    mode = os.environ.get('mode', 'dry_run').strip().lower()

    # Allow query parameter overrides for manual testing
    query_params = event.get('queryStringParameters') or {}
    if 'mode' in query_params:
        mode = query_params.get('mode').strip().lower()

    commit_flag = (mode == "commit" and request_type != "test")
    logger.info("Execution mode: '%s' | Request type: '%s' | Commit enabled: %s", mode, request_type, commit_flag)

    try:
        conn = get_db_connection()
    except Exception as e:
        logger.error("ERROR: Could not connect to MySQL database: %s", str(e))
        sys.exit()

    logger.info("SUCCESS: Connection to MySQL database succeeded")

    player_id = os.environ.get('player_id') or query_params.get('player_id')
    modify = os.environ.get('modify') or query_params.get('modify')

    if player_id:
        try:
            player_id = int(player_id)
        except ValueError:
            conn.close()
            return response(400, 'text/plain', f"Error: Invalid player_id '{player_id}'. Must be an integer.")

    # Branch 1: No player_id specified -> Pretty print entire Players table
    if not player_id:
        logger.info("No player_id specified. Pretty printing entire Players table.")
        output = pretty_print_players(conn)
        conn.close()
        print(output)
        return response(200, 'text/plain', output)

    # Branch 2: player_id specified, but 'modify' is omitted -> Pretty print single player
    if player_id and not modify:
        logger.info("player_id %s specified without 'modify'. Pretty printing player.", player_id)
        if not validate_field(conn, str(player_id), 'player_id', 'Players'):
            conn.close()
            return response(404, 'text/plain', f"Error: Player ID {player_id} not found in database.")

        output = pretty_print_players(conn, player_id=player_id)
        conn.close()
        print(output)
        return response(200, 'text/plain', output)

    # Branch 3: Both player_id and 'modify' are provided -> Perform dynamic validation and update/dry-run
    if ':' not in modify:
        conn.close()
        return response(400, 'text/plain', "Error: 'modify' variable must be formatted as '<field name>:<value>'.")

    field_name, raw_value = modify.split(':', 1)
    field_name = field_name.strip()
    raw_value = raw_value.strip()

    # Validate that the target player exists
    if not validate_field(conn, str(player_id), 'player_id', 'Players'):
        conn.close()
        return response(404, 'text/plain', f"Error: Player ID {player_id} not found in database.")

    # Fetch dynamic live schema
    live_schema = fetch_live_table_schema(conn)

    # Validate input value against schema definitions
    is_valid, casted_value, msg = validate_and_cast_dynamic(field_name, raw_value, live_schema)
    if not is_valid:
        logger.error("Dynamic schema validation failed: %s", msg)
        conn.close()
        return response(400, 'text/plain', f"Validation Error: {msg}")

    exact_col_name = next(c for c in live_schema.keys() if c.lower() == field_name.lower())

    # Branch 3A: Execute and Commit
    if commit_flag:
        try:
            with conn.cursor() as cur:
                sql = f"UPDATE `Players` SET `{exact_col_name}` = %s WHERE `player_id` = %s"
                logger.info("Executing: %s with values (%s, %s)", sql, casted_value, player_id)
                cur.execute(sql, (casted_value, player_id))
                conn.commit()
        except Exception as e:
            logger.error("Error executing database update: %s", str(e))
            conn.close()
            return response(500, 'text/plain', f"Database Error: {str(e)}")

        logger.info("COMMITTED: Updated player %s setting `%s` = %s", player_id, exact_col_name, casted_value)
        updated_output = pretty_print_players(conn, player_id=player_id)
        conn.close()

        result_text = (
            f"Status: COMMITTED\n"
            f"Updated player {player_id} setting `{exact_col_name}` = {casted_value}\n\n"
            f"{updated_output}"
        )
        print(result_text)
        return response(200, 'text/plain', result_text)

    # Branch 3B: Dry-Run / Simulation
    else:
        logger.info(
            "DRY-RUN: Would update player %s setting `%s` = %s (no changes committed)",
            player_id, exact_col_name, casted_value
        )
        current_output = pretty_print_players(conn, player_id=player_id)
        conn.close()

        result_text = (
            f"Status: DRY-RUN (Simulated - no changes committed)\n"
            f"Mode: '{mode}' | Request Type: '{request_type}'\n"
            f"Would update player {player_id}: set `{exact_col_name}` = {casted_value}\n\n"
            f"Current record in database:\n{current_output}"
        )
        print(result_text)
        return response(200, 'text/plain', result_text)
