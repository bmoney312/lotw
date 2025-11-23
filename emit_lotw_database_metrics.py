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
from lotw import get_current_year, in_daylight_savings

# global variables
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)



def lambda_handler(event, context):
    """
    Emit LOTW database metrics to CloudWatch
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

    logger.info("Instantiating cloudwatch object")
    cloudwatch = boto3.client('cloudwatch')
    
    # initialize variables
    #mail_username = os.environ['mail_username']
    #mail_password = os.environ['mail_password']
    #mail_host = os.environ['mail_host']
    #mail_port = os.environ['mail_port']
    #mail_from = '"Brendan Connell" <bmoney312@gmail.com>'

    # determine current week
    week = get_current_week(conn)

    if week is None:
        logger.error("ERROR: Unable to determine current week!")
        sys.exit()
    else:
        week = int(week)

    # current time, set second and microsecond to 0 to match kickoff times
    time_now = datetime.datetime.now().replace(second=0, microsecond=0)

    logger.info("Current week set to {}".format(week))
    logger.info("Current time is {}".format(time_now))

    logger.info("Gathering database metrics...")

    #weekday = time_now.weekday()
    #dst = in_daylight_savings()

    # get standings and current picks
    #player_picks = get_picks_at_kickoff_time(conn, week, pick_deadline, send_pick_summary)
    #logger.debug("player_picks: {}".format(player_picks))
    # get_all_paid_players, get_all_players, get_all_current_players, get_all_picks

    #if len(player_picks) == 0:
    #    return response(200, 'text/html', build_html("No picks found that locked in at {}".format(pick_deadline)))


    paid_players = []
    paid_players = get_all_paid_players(conn)
    num_paid_players = len(paid_players)

    current_players = []
    current_players = get_all_current_players(conn)
    num_current_players = len(current_players)
    num_not_paid_players = num_current_players - num_paid_players

    all_players = []
    all_players = get_all_players(conn)
    num_all_players = len(all_players)

    all_weekly_picks = []
    all_weekly_picks = get_all_picks(conn, week)
    num_weekly_picks = len(num_weekly_picks)
    num_no_picks = num_current_players - num_weekly_picks

#    response = cloudwatch.put_metric_data(
#        MetricData = [
#            {
#                'MetricName': 'KPIs',
#                'Dimensions': [
#                    {
#                        'Name': 'PURCHASES_SERVICE',
#                        'Value': 'CoolService'
#                    },
#                    {
#                        'Name': 'APP_VERSION',
#                        'Value': '1.0'
#                    },
#                ],
#                'Unit': 'None',
#                'Value': random.randint(1, 500)
#            },
#        ],
#        Namespace='lotw'
#    )


    # close database connection
    conn.close()

    # return result
    return response(200, 'text/html', build_html("Metrics published to CloudWatch successfully."))

