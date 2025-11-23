import os
import sys
import json
import pymysql
import logging
import datetime
import pytz
import boto3
from time import sleep
from lotw import get_all_paid_players, get_all_players, get_all_current_players, get_all_picks
from lotw import get_current_year, get_current_week, response, build_html
# Note: Added get_current_week and response/build_html to imports

# global variables
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def put_cloudwatch_metric(cw_client, namespace, metric_name, value, dimensions, unit='Count'):
    """
    Helper function to put a single metric to CloudWatch.
    """
    try:
        cw_client.put_metric_data(
            Namespace=namespace,
            MetricData=[
                {
                    'MetricName': metric_name,
                    'Dimensions': dimensions,
                    'Value': value,
                    'Unit': unit
                },
            ]
        )
        logger.info("Successfully put metric: {} = {}".format(metric_name, value))
    except Exception as e:
        logger.error("Failed to put metric {}: {}".format(metric_name, str(e)))


def lambda_handler(event, context):
    """
    Emit LOTW database metrics to CloudWatch
    """

    logger.info("Received event: " + json.dumps(event, indent=2))

    request_type = event.get('detail-type')
    if request_type is None:
        logger.info("Unable to determine request type, defaulting to 'Scheduled Event'")
        request_type = "Scheduled Event" # Default for metrics

    db_endpoint = os.environ['db_endpoint']
    db_port = int(os.environ['db_port'])
    db_username = os.environ['db_username']
    db_password = os.environ['db_password']
    db_name = db=os.environ['db_name']

    logger.info("Connecting to MySQL database {}".format(db_endpoint))

    try:
        conn = pymysql.connect(host=db_endpoint, port=db_port,
                                user=db_username, passwd=db_password,
                                db=db_name,connect_timeout=5) # Removed DictCursor
    except Exception as e:
        logger.error("ERROR: Unexpected error: Could not connect to MySQL database: {}".format(str(e)))
        sys.exit()

    logger.info("SUCCESS: Connection to MySQL database succeeded")

    logger.info("Instantiating cloudwatch object")
    cloudwatch = boto3.client('cloudwatch')
    cw_namespace = 'lotw'

    # --- Determine Current Year and Week ---
    try:
        current_year = get_current_year()
        current_week = get_current_week(conn)

        if current_week is None:
            logger.error("ERROR: Unable to determine current week!")
            sys.exit()
        else:
            current_week = int(current_week)
        
        metrics_week = current_week - 1 # For "past week" metrics
        
        time_now = datetime.datetime.now().replace(second=0, microsecond=0)
        logger.info("Current year set to {}".format(current_year))
        logger.info("Current week set to {}".format(current_week))
        logger.info("Metrics week set to {}".format(metrics_week))
        logger.info("Current time is {}".format(time_now))

    except Exception as e:
        logger.error("Error determining week/year: {}".format(str(e)))
        conn.close()
        sys.exit()

    logger.info("Gathering database metrics...")
    
    num_current_players = 0 # Initialize player count

    # --- Metric 1: Registered Players Per Year ---
    try:
        with conn.cursor() as cur:
            # Find all registration columns
            cur.execute("SHOW COLUMNS FROM Players LIKE '%\_registration'")
            reg_cols = cur.fetchall()

            for col in reg_cols:
                col_name = col[0] # Changed from col['Field']
                year_str = col_name.split('_')[0]
                
                # Get count of registered players for that year
                cur.execute("SELECT COUNT(*) AS count FROM Players WHERE `{}` = 1".format(col_name))
                count = cur.fetchone()[0] # Changed from ['count']
                
                put_cloudwatch_metric(
                    cloudwatch, cw_namespace, 'RegisteredPlayers', count,
                    [{'Name': 'Year', 'Value': year_str}]
                )
    except Exception as e:
        logger.error("Failed to get registration metrics: {}".format(str(e)))

    # --- Metrics 2 & 3: Picks Made / No Picks (Current Week) ---
    try:
        # These helpers are from the lotw.py file
        current_players = get_all_current_players(conn) # Registered for this year
        all_weekly_picks = get_all_picks(conn, current_week) # Valid picks for this week
        
        num_current_players = len(current_players) # Set for use in metric 4
        num_weekly_picks = len(all_weekly_picks) # Bug fixed from skeleton
        num_no_picks = num_current_players - num_weekly_picks

        year_dim = [{'Name': 'Year', 'Value': str(current_year)}]
        week_dims = [
            {'Name': 'Year', 'Value': str(current_year)},
            {'Name': 'Week', 'Value': str(current_week)}
        ]

        # Total registered players for the current year
        put_cloudwatch_metric(cloudwatch, cw_namespace, 'CurrentPlayers', num_current_players, year_dim)

        # Picks metrics for the current week
        put_cloudwatch_metric(cloudwatch, cw_namespace, 'PicksMade', num_weekly_picks, week_dims)
        put_cloudwatch_metric(cloudwatch, cw_namespace, 'PlayersWithoutPick', num_no_picks, week_dims)

    except Exception as e:
        logger.error("Failed to get weekly pick metrics: {}".format(str(e)))

    # --- Metrics 4 & 5: Winners, Losers & Team W/L (Past Week) ---
    if metrics_week > 0:
        picks_table = "Picks_" + str(current_year)
        games_table = "Games_" + str(current_year)
        
        # Metric 4: Number and percentage of winners/losers
        try:
            with conn.cursor() as cur:
                # Get winning picks (pick_ats > 0)
                cur.execute("SELECT COUNT(*) AS count FROM {} WHERE week = %s AND pick_ats > 0".format(picks_table), (metrics_week,))
                total_winners = cur.fetchone()[0] # Changed from ['count']
                
                # Get losing picks (pick_ats <= 0)
                cur.execute("SELECT COUNT(*) AS count FROM {} WHERE week = %s AND pick_ats <= 0".format(picks_table), (metrics_week,))
                losers_who_picked = cur.fetchone()[0] # Changed from ['count']

                # Get total picks made
                cur.execute("SELECT COUNT(*) AS count FROM {} WHERE week = %s AND lock_in_time IS NOT NULL".format(picks_table), (metrics_week,))
                total_picks_made = cur.fetchone()[0] # Changed from ['count']
                
                # Calculate players who did not make a pick (counts as a loss)
                # Uses num_current_players set in Metric 2/3 block
                no_picks_past_week = num_current_players - total_picks_made
                
                # Total losers = (players who picked and lost) + (players who didn't pick)
                total_losers = losers_who_picked + no_picks_past_week
                
                # Calculate win percentage based only on picks made
                percentage = (total_winners / total_picks_made) * 100 if total_picks_made > 0 else 0

                dims = [
                    {'Name': 'Year', 'Value': str(current_year)},
                    {'Name': 'Week', 'Value': str(metrics_week)}
                ]
                put_cloudwatch_metric(cloudwatch, cw_namespace, 'WeeklyWinners', total_winners, dims)
                put_cloudwatch_metric(cloudwatch, cw_namespace, 'WeeklyLosers', total_losers, dims)
                put_cloudwatch_metric(cloudwatch, cw_namespace, 'WeeklyNoPicks', no_picks_past_week, dims)
                put_cloudwatch_metric(cloudwatch, cw_namespace, 'WeeklyWinPercentage', percentage, dims, unit='Percent')

        except Exception as e:
            logger.error("Failed to get weekly winner/loser metrics: {}".format(str(e)))

        # Metric 5: Wins and losses per team
        try:
            team_wins = {}
            team_losses = {}
            with conn.cursor() as cur:
                cur.execute("SELECT home_team_id, home_team_ats, away_team_id, away_team_ats FROM {} WHERE week = %s".format(games_table), (metrics_week,))
                games = cur.fetchall()

            for game in games:
                # Indices: 0=home_team_id, 1=home_team_ats, 2=away_team_id, 3=away_team_ats
                if game[1] is not None: # home_team_ats
                    if game[1] > 0: # home_team_ats
                        team_wins[game[0]] = team_wins.get(game[0], 0) + 1 # home_team_id
                    else:
                        team_losses[game[0]] = team_losses.get(game[0], 0) + 1 # home_team_id
                
                if game[3] is not None: # away_team_ats
                    if game[3] > 0: # away_team_ats
                        team_wins[game[2]] = team_wins.get(game[2], 0) + 1 # away_team_id
                    else:
                        team_losses[game[2]] = team_losses.get(game[2], 0) + 1 # away_team_id
            
            # Put metrics for each team
            for team_id, count in team_wins.items():
                dims = [{'Name': 'Year', 'Value': str(current_year)}, {'Name': 'Week', 'Value': str(metrics_week)}, {'Name': 'Team', 'Value': team_id}]
                put_cloudwatch_metric(cloudwatch, cw_namespace, 'TeamGameWins', count, dims)
            
            for team_id, count in team_losses.items():
                dims = [{'Name': 'Year', 'Value': str(current_year)}, {'Name': 'Week', 'Value': str(metrics_week)}, {'Name': 'Team', 'Value': team_id}]
                put_cloudwatch_metric(cloudwatch, cw_namespace, 'TeamGameLosses', count, dims)

        except Exception as e:
            logger.error("Failed to get team W/L metrics: {}".format(str(e)))

    # --- Metric 6: Games Played / Remaining (Current Year) ---
    try:
        games_table = "Games_" + str(current_year)
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS count FROM {} WHERE away_team_score IS NOT NULL".format(games_table))
            played = cur.fetchone()[0] # Changed from ['count']
            
            cur.execute("SELECT COUNT(*) AS count FROM {} WHERE away_team_score IS NULL".format(games_table))
            remaining = cur.fetchone()[0] # Changed from ['count']
            
            cur.execute("SELECT COUNT(*) AS count FROM {}".format(games_table))
            total = cur.fetchone()[0] # Changed from ['count']
            
            dims = [{'Name': 'Year', 'Value': str(current_year)}]
            put_cloudwatch_metric(cloudwatch, cw_namespace, 'GamesPlayed', played, dims)
            put_cloudwatch_metric(cloudwatch, cw_namespace, 'GamesRemaining', remaining, dims)
            put_cloudwatch_metric(cloudwatch, cw_namespace, 'TotalGames', total, dims)
    
    except Exception as e:
        logger.error("Failed to get game count metrics: {}".format(str(e)))


    # close database connection
    conn.close()

    # return result
    logger.info("Metrics published to CloudWatch successfully.")
    return response(200, 'text/html', build_html("Metrics published to CloudWatch successfully."))
