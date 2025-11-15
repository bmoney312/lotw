import os
import sys
import json
import pymysql
import logging
import datetime
from lotw import update_game_ats, update_pick_ats, validate_field, get_current_year
from lotw import get_all_player_picks, get_all_current_players, get_player, get_standings
from lotw import get_current_week, build_html, response

# global variables
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def update_standings_table(conn, week):
    """
    Update Standings table based on picks thru and including week provided
    
    For each player in Players table, compute wins / losses / win% / 
    ATS for [week] and update Standings

    returns nothing
    """

    all_players = get_all_current_players(conn)

    for player in all_players:
        (player_id, player_email, last_name, first_name, titles, is_rookie) = player
        logger.debug("update_standings_table(): working on player {} {} {} {}".format(player_id, first_name, last_name, player_email))

        # add player if not yet in Standings table
        new_standings_entry = False
        if not validate_field(conn, player_id, 'player_id', "Standings_" + str(get_current_year())):
            new_standings_entry = True

        # compute Standings table entry for player
        player_picks = []
        if week != 0:
            player_picks = get_all_player_picks(conn, player_id, week)
            total_picks = len(player_picks)
        else:
            total_picks = 0
        
        player_wins = 0
        player_losses = 0
        player_ats = 0
        player_win_percentage = 0.000

        # account for past no picks
        if total_picks < week:
            no_picks = week - total_picks
            player_losses += no_picks
            logger.debug("Player {} has {} NO PICKs".format(player_id, no_picks))

        for pick in player_picks:
            (pick_week, pick_team_id, pick_ats) = pick
            # tally of ATS points
            player_ats += pick_ats
            # win, tie ATS counts as loss
            if pick_ats > 0:
                player_wins += 1
            else:
                player_losses += 1

        # compute win percentage
        if week == 0:
            player_win_percentage = 0.000
        else:
            player_win_percentage = player_wins / week

        logger.info("Computed {} Ws / {} Ls / {} ATS / {} Win% for player {}".format(player_wins, player_losses, player_ats, player_win_percentage, player_id))

        # update Standings table
        try:
            with conn.cursor() as cur:
                if new_standings_entry:
                    sql = "INSERT INTO `Standings_" + str(get_current_year()) + "` (`player_id`, `wins`, `losses`, `win_percentage`, `ats_points`) VALUES (%s, %s, %s, %s, %s)"
                    cur.execute(sql, (player_id, player_wins, player_losses, player_win_percentage, player_ats))
                else:
                    sql = "UPDATE `Standings_" + str(get_current_year()) + "` SET `wins`=%s, `losses`=%s, `win_percentage`=%s, `ats_points`=%s WHERE `player_id` = %s"
                    cur.execute(sql, (player_wins, player_losses, player_win_percentage, player_ats, player_id))

                logger.debug("update_standings_table(): ".format(sql))
                conn.commit()
        except Exception as e:
            logger.error("Error updating database: {}".format(str(e)))
            raise

        logger.debug("Updated standings table for player {}".format(player_id))



def lambda_handler(event, context):
    """
    Email LOTW standings to each player each week
    """

    logger.info("Received event: " + json.dumps(event, indent=2))

    request_type = event.get('detail-type')
    if request_type is None:
        logger.error("Unable to determine request type")
        sys.exit()

    db_endpoint = os.environ['db_endpoint']
    db_port = int(os.environ['db_port'])
    db_username = os.environ['db_username']
    db_password = os.environ['db_password']
    db_name = db=os.environ['db_name']

    logger.info("Connecting to MySQL database {}".format(db_endpoint))

    try:
        conn = pymysql.connect(host=db_endpoint, port=db_port,
                                user=db_username, passwd=db_password,
                                db=db_name,connect_timeout=5)
    except:
        logger.error("ERROR: Unexpected error: Could not connect to MySQL database")
        sys.exit()

    logger.info("SUCCESS: Connection to MySQL database succeeded")
    
    if request_type == "Scheduled Event":
        logger.debug("Scheduled Event")
        #players = get_all_current_players(conn)
    elif request_type == "test":
        logger.debug("test")
        #players = get_player(conn, int(1))
    elif request_type == "manual_run":
        logger.debug("manual_run")
        #players = get_all_current_players(conn)
    else:
        logger.error("Invalid request type {}".format(request_type))
        sys.exit()

    logger.info("Request type is {}".format(request_type))

    # week to compute standings, set to last week unless
    # environment variable week set then use same week
    standings_week = 0

    # determine current week
    week = os.environ.get('week')

    # if week is not provided
    if week is None:
        week = get_current_week(conn)
        if week is None:
            logger.error("ERROR: Unable to determine current week!")
            sys.exit()
        standings_week = int(week) - 1
    else:
        standings_week = int(week)

    logger.info("Current week set to {}".format(week))
    logger.info("Standings week set to {}".format(standings_week))
    logger.info("Current time is {}".format(datetime.datetime.now()))

    # compute standings
    logger.info("Updating game ATS values")
    (result, message) = update_game_ats(conn, standings_week)
    if result is not True:
        return response(200, 'text/html', build_html("Update of game ATS failed for week {}: {}".format(week, message)))

    logger.info("Updating pick ATS values")
    (result, message) = update_pick_ats(conn, standings_week)
    if result is not True and standings_week > 0:
        return response(200, 'text/html', build_html("Update of pick ATS failed for week {}: {}".format(week, message)))

    if standings_week == 0:
        logger.info("Continuing with update because standings week is 0")

    # update Standings table in database
    logger.info("Updating standings table")
    update_standings_table(conn, standings_week)

    # close database connection
    conn.close()

    # return result
    return response(200, 'text/html', build_html("Standings for week {} updated successfully.".format(standings_week)))

