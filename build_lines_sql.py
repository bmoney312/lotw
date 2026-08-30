import os
import sys
import json
import pymysql
import logging
from lotw import get_current_week, get_current_year, get_all_games, response

# global variables
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def generate_sql_lines(conn, week):
    """
    Generate SQL UPDATE statements for the given week to set lines.
    Returns a string containing the SQL commands.
    """
    year = get_current_year()
    table_name = "Games_" + str(year)
    
    # Fetch all games for the specific week
    # get_all_games returns tuples of:
    # (game_id, week, kickoff_time, away_team_id, home_team_id, home_team_line, ...)
    try:
        games = get_all_games(conn, week)
    except Exception as e:
        logger.error("Error fetching games from DB: {}".format(str(e)))
        return None

    if not games:
        logger.warning("No games found for week {}".format(week))
        return ""
        
    sql_output = ""
    
    for game in games:
        game_id = game[0]
        home_team_id = game[4]
        
        # Format the SQL string leaving home_team_line empty as requested
        # Example: UPDATE Games_2025 SET home_team_line = WHERE game_id = 136 AND home_team_id = 'DEN';
        sql_line = "UPDATE {} SET home_team_line = WHERE game_id = {} AND home_team_id = '{}';".format(
            table_name, game_id, home_team_id
        )
        sql_output += sql_line + "<br>"
        
    return sql_output


def lambda_handler(event, context):
    """
    Generate a SQL file for updating game lines
    """

    logger.info("Received event: " + json.dumps(event, indent=2))

    request_type = event.get('detail-type')
    
    # Allow for direct invocation or API Gateway
    if request_type is None:
         # Check for direct invocation parameters or default to manual
         request_type = event.get('requestContext', {}).get('stage', 'manual_run')


    db_endpoint = os.environ['db_endpoint']
    db_port = int(os.environ['db_port'])
    db_username = os.environ['db_username']
    db_password = os.environ['db_password']
    db_name = os.environ['db_name']

    logger.info("Connecting to MySQL database {}".format(db_endpoint))

    try:
        conn = pymysql.connect(host=db_endpoint, port=db_port,
                                user=db_username, passwd=db_password,
                                db=db_name, connect_timeout=5)
    except Exception as e:
        logger.error("ERROR: Unexpected error: Could not connect to MySQL database: {}".format(str(e)))
        sys.exit()

    logger.info("SUCCESS: Connection to MySQL database succeeded")
    
    # Determine target week
    week = os.environ.get('week')
    
    # Check API Gateway query params if they exist
    query_string_params = event.get('queryStringParameters')
    if query_string_params is not None and 'week' in query_string_params:
        week = query_string_params.get('week')

    if week is None:
        logger.info("No 'week' in env or params, attempting to get current week.")
        week = get_current_week(conn)
    else:
        week = int(week)

    if week is None:
        logger.error("ERROR: Unable to determine week!")
        conn.close()
        return response(400, 'text/plain', "Error: Unable to determine week.")

    logger.info("Generating SQL for week {}".format(week))

    # Generate the content
    try:
        sql_content = generate_sql_lines(conn, week)
        if sql_content is None:
             raise Exception("Failed to generate SQL lines.")
             
    except Exception as e:
        logger.error("Error generating SQL: {}".format(str(e)))
        conn.close()
        return response(500, 'text/plain', "Error generating SQL: {}".format(str(e)))

    # Close database connection
    conn.close()

    # Return result as plain text so it can be easily copied/saved as .sql
    sql_html = "<html><body>" + sql_content + "</body></html>"
    return response(200, 'text/html', sql_html)
