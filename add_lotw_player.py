import os
import sys
import pymysql
import logging
from lotw import validate_field, get_current_year, build_html, response

# global variables
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def add_lotw_player(conn, email, first_name, last_name, testflag):
    """
    Add new player to Lock of the Week
    Returns tuple of new player_id + message upon success, tuple of None + error message upon failure
    """
    # first confirm player email does not exist in Players table
    result = validate_field(conn, str(email), str("email"), str("Players"))
    if result is True:
        return (False, "Player email {} already exists!".format(email))

    logger.info("Adding new player: {} {}, {}".format(first_name, last_name, email))
    current_year = get_current_year()
    try:
        with conn.cursor() as cur:
            sql = "INSERT INTO `Players` (`email`, `last_name`, `first_name`, `past_titles`, `rookie`, `" + str(current_year) + "_registration`, `" + str(current_year) + "_paid`) VALUES (%s, %s, %s, 0, 1, NULL, NULL)"
            logger.debug(sql)
            if testflag is True:
                logger.info("Test flag detected, not committing change. SQL statement: {}".format(sql))
            else:
                cur.execute(sql, (str(email), str(last_name), str(first_name),))
                conn.commit()
    except Exception as e:
        logger.error("Error updating database: {}".format(str(e)))
        raise

    message = "Successfully added player {} {}, {} to Players table".format(first_name, last_name, email)
    logger.info(message)
    return (True, message)


def lambda_handler(event, context):
    """
    Add new player to Lock of the Week
    """

    request_type = event.get('detail-type')
    if request_type is None:
        logger.error("Unable to determine request type")
        sys.exit()

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
        logger.error("ERROR: Unexpected error: Could not connect to MySQL database - {}".format(str(e)))
        sys.exit()

    logger.info("SUCCESS: Connection to MySQL database succeeded")

    # initialize variables
    email = os.environ.get('email')
    first_name = os.environ.get('first_name')
    last_name = os.environ.get('last_name')

    testflag = False
    if request_type == "test":
        logger.debug("test")
        testflag = True
    else:
        logger.debug("manual_run")
        testflag = False

    # add new player
    (result, message) = add_lotw_player(conn, email, first_name, last_name, testflag)

    # close database connection
    conn.close()

    # return result
    if result is True:
        return response(200, 'text/html', build_html(message))
    else:
        return response(400, 'text/html', build_html(message))
