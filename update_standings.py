import os
import sys
import json
import logging
import datetime
from lotw import update_game_ats, update_pick_ats, validate_field, get_current_year
from lotw import get_current_week
from lotw import build_html, response, get_db_connection

# global variables
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_all_player_picks_by_year(conn, player_id, week, year):
    """
    For given player get all valid picks up to week for the specified year.
    Returns list of tuples containing (week, pick, pick_ats).
    """
    with conn.cursor() as cur:
        select_statement = (
            f"SELECT `week`, `pick`, `pick_ats` FROM `Picks_{year}` "
            "WHERE `player_id` = %s AND `week` <= %s AND `lock_in_time` IS NOT NULL"
        )
        cur.execute(select_statement, (player_id, week))
        return cur.fetchall()


def get_all_current_players_by_year(conn, year):
    """
    Return all rows in LOTW Players database
    who are registered to play in the specified year.
    """
    with conn.cursor() as cur:
        select_statement = (
            "SELECT `player_id`, `email`, `last_name`, `first_name`, `past_titles`, `rookie` "
            "FROM Players WHERE `" + str(year) + "_registration` = 1"
        )
        cur.execute(select_statement)
        return cur.fetchall()


def get_player_streak(conn, player_id, current_standings_week, year):
    """
    Calculate the player's current win/loss streak working backwards from the most recent week.
    """
    if current_standings_week == 0:
        return "-"

    try:
        all_picks = get_all_player_picks_by_year(conn, player_id, current_standings_week, year)
        picks_map = {
            week: ats for week, pick, ats in all_picks
            if ats is not None and week <= current_standings_week
        }

        streak_count = 0
        streak_type = None

        for week in range(current_standings_week, 0, -1):
            ats_result = picks_map.get(week)

            current_result_type = 'W' if (ats_result is not None and ats_result > 0) else 'L'

            if streak_type is None:
                streak_type = current_result_type
                streak_count = 1
            elif current_result_type == streak_type:
                streak_count += 1
            else:
                break

        if streak_type is None:
            return "-"

        return f"{streak_type}{streak_count}"

    except Exception as e:
        logger.error("Error calculating streak for player %s: %s", player_id, str(e))
        return "?"


def update_standings_table(conn, week, year):
    """
    Update Standings table based on picks thru and including week provided.
    """

    # Velocity check threshold
    MAX_PRUNE_LIMIT = 5

    # Prune players who are no longer registered with a velocity safety check
    try:
        with conn.cursor() as cur:
            count_sql = (
                "SELECT COUNT(*) FROM `Standings_{}` "
                "WHERE player_id NOT IN ("
                "SELECT player_id FROM Players WHERE `{}_registration` = 1"
                ")"
            ).format(year, year)
            logger.debug("update_standings_table(): checking prune candidate count SQL: {}".format(count_sql))
            cur.execute(count_sql)
            prune_count = cur.fetchone()[0]

            # Velocity check: abort if count exceeds threshold
            if prune_count > MAX_PRUNE_LIMIT:
                error_msg = (
                    "Velocity check failed: {} players flagged for pruning in Standings_{}, "
                    "which exceeds the safety limit of {}. Aborting standings update."
                ).format(prune_count, year, MAX_PRUNE_LIMIT)
                logger.error(error_msg)
                raise RuntimeError(error_msg)

            if prune_count > 0:
                prune_sql = (
                    "DELETE FROM `Standings_{}` "
                    "WHERE player_id NOT IN ("
                    "SELECT player_id FROM Players WHERE `{}_registration` = 1"
                    ")"
                ).format(year, year)
                logger.debug("update_standings_table(): pruning unregistered players SQL: {}".format(prune_sql))
                cur.execute(prune_sql)
                conn.commit()
                logger.info("Pruned {} unregistered player(s) from Standings_{}".format(prune_count, year))
            else:
                logger.debug("No unregistered players to prune from Standings_{}".format(year))

    except Exception as e:
        logger.error("Error during prune safety check / execution for Standings_{}: {}".format(year, str(e)))
        raise

    all_players = get_all_current_players_by_year(conn, year)

    for player in all_players:
        (player_id, player_email, last_name, first_name, titles, is_rookie) = player
        logger.debug("update_standings_table(): working on player %s %s %s %s", player_id, first_name, last_name, player_email)

        new_standings_entry = False
        if not validate_field(conn, player_id, 'player_id', f"Standings_{year}"):
            new_standings_entry = True

        player_picks = []
        if week != 0:
            player_picks = get_all_player_picks_by_year(conn, player_id, week, year)
            total_picks = len(player_picks)
        else:
            total_picks = 0

        player_wins = 0
        player_losses = 0
        player_ats = 0
        player_win_percentage = 0.000

        if total_picks < week:
            no_picks = week - total_picks
            player_losses += no_picks
            logger.debug("Player %s has %s NO PICKs", player_id, no_picks)

        for pick in player_picks:
            (pick_week, pick_team_id, pick_ats) = pick

            # Guard against NULL pick_ats in database
            ats_val = pick_ats if pick_ats is not None else 0
            player_ats += ats_val

            # Win (>0), Tie/Push (0), or Loss (<0) / Missing counts as loss
            if pick_ats is not None and pick_ats > 0:
                player_wins += 1
            else:
                player_losses += 1

        if week == 0:
            player_win_percentage = 0.000
        else:
            player_win_percentage = player_wins / week

        logger.info("Computed %s Ws / %s Ls / %s ATS / %s Win%% for player %s", player_wins, player_losses, player_ats, player_win_percentage, player_id)

        streak_string = get_player_streak(conn, player_id, week, year)
        logger.info("Computed streak %s for player %s", streak_string, player_id)

        try:
            with conn.cursor() as cur:
                if new_standings_entry:
                    sql = (
                        f"INSERT INTO `Standings_{year}` "
                        "(`player_id`, `wins`, `losses`, `win_percentage`, `ats_points`, `streak`) "
                        "VALUES (%s, %s, %s, %s, %s, %s)"
                    )
                    cur.execute(sql, (player_id, player_wins, player_losses, player_win_percentage, player_ats, streak_string))
                else:
                    sql = (
                        f"UPDATE `Standings_{year}` "
                        "SET `wins`=%s, `losses`=%s, `win_percentage`=%s, `ats_points`=%s, `streak`=%s "
                        "WHERE `player_id` = %s"
                    )
                    cur.execute(sql, (player_wins, player_losses, player_win_percentage, player_ats, streak_string, player_id))

                logger.debug("update_standings_table(): %s", sql)
                conn.commit()
        except Exception as e:
            logger.error("Error updating database: %s", str(e))
            raise

        logger.debug("Updated standings table for player %s", player_id)


def lambda_handler(event, context):
    """
    Update LOTW standings for the specified week and year.
    """
    logger.info("Received event: %s", json.dumps(event, indent=2))

    request_type = event.get('detail-type')
    if request_type is None:
        logger.error("Unable to determine request type")
        sys.exit()

    try:
        conn = get_db_connection()
    except Exception as e:
        logger.error("ERROR: Could not connect to MySQL database: %s", str(e))
        sys.exit()

    logger.info("SUCCESS: Connection to MySQL database succeeded")

    if request_type == "Scheduled Event":
        logger.debug("Scheduled Event")
    elif request_type == "test":
        logger.debug("test")
    elif request_type == "manual_run":
        logger.debug("manual_run")
    else:
        logger.error("Invalid request type %s", request_type)
        sys.exit()

    logger.info("Request type is %s", request_type)

    current_year = get_current_year()
    year_env = os.environ.get('year')

    if year_env is not None and year_env != '':
        try:
            year = int(year_env)
        except ValueError:
            error_msg = f"Invalid year value: '{year_env}' is not an integer"
            logger.error(error_msg)
            conn.close()
            return response(400, 'text/html', build_html(error_msg))

        if not (2000 < year <= current_year):
            error_msg = f"Invalid year: {year}. Year must be > 2000 and <= current year ({current_year})"
            logger.error(error_msg)
            conn.close()
            return response(400, 'text/html', build_html(error_msg))
    else:
        year = current_year

    logger.info("Operating on year: %s", year)

    standings_week = 0

    if year < current_year:
        standings_week = 21 if year < 2021 else 22
        week = standings_week
        logger.info("Previous year %s detected. Setting week to %s", year, standings_week)
    else:
        week = os.environ.get('week')
        if week is None:
            week = get_current_week(conn)
            if week is None:
                logger.error("ERROR: Unable to determine current week!")
                conn.close()
                sys.exit()
            standings_week = int(week) - 1
        else:
            standings_week = int(week)

    logger.info("Current week set to %s", week)
    logger.info("Standings week set to %s", standings_week)
    logger.info("Current time is %s", datetime.datetime.now())

    if year == current_year:
        logger.info("Updating game ATS values")
        (result, message) = update_game_ats(conn, standings_week)
        if result is not True:
            conn.close()
            return response(200, 'text/html', build_html(f"Update of game ATS failed for week {week}: {message}"))

        logger.info("Updating pick ATS values")
        (result, message) = update_pick_ats(conn, standings_week)
        if result is not True and standings_week > 0:
            conn.close()
            return response(200, 'text/html', build_html(f"Update of pick ATS failed for week {week}: {message}"))

        if standings_week == 0:
            logger.info("Continuing with update because standings week is 0")
    else:
        logger.info("Skipping game and pick ATS updates for previous year %s (data already backfilled)", year)

    logger.info("Updating standings table")
    update_standings_table(conn, standings_week, year)

    conn.close()
    return response(200, 'text/html', build_html(f"Standings for week {standings_week} updated successfully."))
