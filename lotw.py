import os
import sys
import json
import pymysql
import logging
import datetime
from dateutil import tz
import pytz
import smtplib
import email.message
import string
import random

# global variables
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def response(status, content_type, response_body, cors=False):
    """
    Return HTTP response, including response code (status), headers and body
    """

    binary_types = [
        'application/octet-stream',
        'application/x-tar',
        'application/zip',
        'image/png',
        'image/jpeg',
        'image/tiff',
        'image/webp'
        ]

    messageData = {
        'statusCode': status,
        'body': response_body,
        'headers': {'Content-Type': content_type}
    }

    if cors:
        messageData['headers']['Access-Control-Allow-Origin'] = '*'
        messageData['headers']['Access-Control-Allow-Methods'] = 'GET'
        messageData['headers']['Access-Control-Allow-Credentials'] = 'true'

    if content_type in binary_types:
        messageData['isBase64Encoded'] = True

    return messageData



def build_html_head():
    """
    Build lines html head
    """

    html = """
<html>
<head>
  <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        .inline {
          display: inline;
        }

        .message {
          display: inline;
          font-size: 2em;
        }

h1 {
    font-size: 2.5rem;
}

h2 {
    font-size: 2.25rem;
}
  
h3 {
    font-size: 2rem;
}

h4 {
    font-size: 1.75rem;
}

h5 {
    font-size: 1.5rem;
}

h6 {
    font-size: 1.25rem;
}

@media (max-width: 480px) {
    html {
        font-size: 12px;
    }
}

@media (min-width: 480px) {
    html {
        font-size: 13px;
    }
}

@media (min-width: 768px) {
    html {
        font-size: 24px;
    }
}

@media (min-width: 992px) {
    html {
        font-size: 15px;
    }
}

@media (min-width: 1200px) {
    html {
        font-size: 16px;
    }
}

        body {
            margin: 20;
            font-family: "Arial", "Helvetica", sans-serif;
        }
        table {
            border-collapse: collapse;
            border: 1px solid black;
        }
        th {
            border: 1px solid black;
            padding: 6px;
            text-align: left;
            background-color: lightgrey;
        }
        td {
            border: 1px solid black;
            padding: 6px;
            text-align: left;
        }
     </style>
</head>
"""
    return html



def build_html_response(message):
    html = build_html_head()
    html += "<body>"
    html += "<h5>{}</h5>".format(message)
    html += "</body></html>"
    return html


def build_html_message(message):
    html = build_html_head()
    html = "<body>"
    html += message
    html += "</body></html>"
    return html



def get_current_pick(conn, player_id, week):
    """
    Given a database connection object, player_id and week, return current pick.
    Note that players can have multiple recorded picks per week so latest pick 
    is always current which has lock_in_time of not NULL.
    
    Value of NULL in database for pick is equivalent to None in Python.
    
    Returns tuple with (pick_id, pick, line, pick_ats, locked_in) locked_in is boolean 
    value showing if pick is locked in (current time after kickoff time).  If picked 
    is locked, return true if not return false.
    
    Example: (000001, DET, 3, None, True)       # pick is locked in but result not recorded
    Example: (000001, DET, 3, -12, True)        # pick is locked in and result recorded
    Example: (000001, None, None, None, False)  # no pick
    Example: (000001, NOP, None, None, False)   # no pick (NOP db entry)
    """

    with conn.cursor() as cur:
        select_statement = "SELECT `pick_id`, `pick`, `pick_ats`, `submit_time`, `lock_in_time` FROM Picks_" + str(get_current_year()) + " WHERE `player_id`=%s AND `week`=%s AND `lock_in_time` IS NOT NULL ORDER BY submit_time DESC"
        logger.debug("get_current_pick(): checking pick for player_id {} week {}".format(player_id, week))
        logger.debug(select_statement)
        cur.execute(select_statement, (player_id, week))
        row = cur.fetchone()
        if row is not None:
            (pick_id, recorded_pick, pick_ats, submit_time, lock_in_time) = row
            logger.debug("Row: {} {} {} {} {}".format(pick_id, recorded_pick, pick_ats, submit_time, lock_in_time))
            recorded_line = get_line(conn, recorded_pick, week)
            if recorded_line is None:
                logger.error("Unexpected return value None checking line for {} week {}".format(recorded_pick, week))
            if datetime.datetime.now() >= lock_in_time:
                logger.debug("Week {} pick {} is locked in for player {} lock in time {}".format(week, recorded_pick, player_id, lock_in_time))
                pick_locked_in = True
            else:
                logger.debug("Week {} pick {} is not locked in yet for player {} lock in time {}".format(week, recorded_pick, player_id, lock_in_time))
                pick_locked_in = False
        else:
            pick_id = None
            recorded_pick = "NOP"
            recorded_line = None
            pick_ats = None
            pick_locked_in = False

    logger.debug("get_current_pick(): player_id {} week {} pick {}".format(player_id, week, recorded_pick))
    return (pick_id, recorded_pick, recorded_line, pick_ats, pick_locked_in)


def get_line(conn, team_id, week):
    """
    Given DB connection, team_id and week, return line for team that week.  If line is not
    posted function returns None.
    """

    with conn.cursor() as cur:
        select_statement = "SELECT `home_team_line`, `away_team_id` FROM Games_" + str(get_current_year()) + " WHERE `away_team_id`=%s and `week`=%s"
        logger.debug("get_line(): checking for away game for team {} week {}".format(team_id, week))
        logger.debug(select_statement)
        cur.execute(select_statement, (team_id, week))
        row = cur.fetchone()
        if row is not None:
            (home_team_line,away_team_id) = row
            if home_team_line is not None:
                away_team_line = -home_team_line
            else:
                away_team_line = None
            return away_team_line 

        select_statement = "SELECT `home_team_line`, `home_team_id` FROM Games_" + str(get_current_year()) + " WHERE `home_team_id`=%s and `week`=%s"
        logger.debug("get_line(): checking for home game for team {} week {}".format(team_id, week))
        logger.debug(select_statement)
        cur.execute(select_statement, (team_id, week))
        row = cur.fetchone()
        if row is not None:
            (home_team_line,home_team_id) = row
            return home_team_line

        logger.debug("get_line(): No game found for team {} week {}".format(team_id, week))
        return None
        #raise ValueError("No game found for team {} week {}".format(team_id, week))



def get_kickoff_time(conn, team_id, week):
    """
    Given DB connection, team_id, and week return kickoff time for team that week.
    """

    with conn.cursor() as cur:
        select_statement = "SELECT `kickoff_time` FROM Games_" + str(get_current_year()) + " WHERE `week`=%s AND (`away_team_id`=%s OR `home_team_id`=%s)"
        logger.debug("get_kickoff_time(): {}".format(select_statement))
        cur.execute(select_statement, (week, team_id, team_id))
        row = cur.fetchone()
        if row is None:
            logger.debug("get_kickoff_time(): did not find any results for team {} week {}".format(team_id, week))
            return None

        (kickoff_time,) = row

        # confirm only one result returned
        row = cur.fetchone()
        if row is not None:
            logger.debug("Multiple results returned for team {} week {}".format(team_id, week))
            raise

        return kickoff_time


def validate_key(d, key):
    """
    Check if key is member of dict
    """

    try:
        value = d[key]
    except KeyError:
        return False
    except:
        raise

    return True


def validate_field(conn, value, field, table):
    """
    Validate 'value' is valid value of field in database table 'table'.
    Return true or false.
    """
    with conn.cursor() as cur:
        select_statement = "SELECT " + field + " FROM " + table + " WHERE " + field + "=%s"
        logger.debug("validate_field(): {}".format(select_statement))
        cur.execute(select_statement, (value,))
        row = cur.fetchone()
        if row is not None:
            return True
        else:
            return False


def formatted_line(line):
    """
    Return string representation of numeric line
    """
    printed_line = ""

    if line is None:
        return printed_line

    if line < 0:
        printed_line = "-{}".format(abs(line))
    elif line == 0:
        printed_line = "PK"
    elif line > 0:
        printed_line = "+{}".format(line)

    return printed_line
    

def build_html(body):
    html = "<html><body>"
    html += body
    html += "</body></html>"
    return html


def smtp_connect(host, port, username, password):
    """
    Given SMTP relay host, port, username and password
    make SMTP connection to host:port, return smtplib.SMTP object
    """

    logger.info("Establishing SMTP connection with {}".format(host))
    try:
        server = smtplib.SMTP(host, port, None, 5)
        server.ehlo()
        server.starttls()
        server.login(username, password)
    except Exception as e:
        logger.error("Error establishing SMTP connection: {}".format(e))
        return None

    return server


def smtp_send(smtp_relay, subject, body, mail_to, mail_from, reply_to = None):
    """
    Given smtplib.SMTP server obj, subject, body, mail_to, mail_from, reply_to as strings
    Send email
    """
    if reply_to is None: reply_to = 'bmoney312@gmail.com'

    msg = email.message.Message()
    msg['Subject'] = subject
    msg['From'] = mail_from
    msg['To'] = ', '.join(mail_to)
    msg['Reply-To'] = reply_to
    msg.add_header('Content-Type','text/html')
    msg.set_payload(body)

    try:
        smtp_relay.sendmail(mail_from, mail_to, msg.as_string())
    except Exception as e:
        logger.error("Error sending email: {}".format(e))
        return False

    return True


def send_email(host, port, username, password, subject, body, mail_to, mail_from = None, reply_to = None):
    """
    Send verification email to player who submitted pick
    """
    if mail_from is None: mail_from = username
    if reply_to is None: reply_to = mail_from

    msg = email.message.Message()
    msg['Subject'] = subject
    msg['From'] = mail_from
    msg['To'] = ', '.join(mail_to)
    msg['Reply-To'] = reply_to
    msg.add_header('Content-Type','text/html')
    msg.set_payload(body)

    try:
        logger.debug("Establishing SMTP connection with {}".format(host))
        logger.debug("message: {}".format(msg.as_string()))
        server = smtplib.SMTP(host, port, None, 5)
        server.ehlo()
        server.starttls()
        server.login(username, password)
        server.sendmail(mail_from, mail_to, msg.as_string())
        server.close()
        return True
    except Exception as e:
        logger.error("Error sending email: {}".format(e))
        return False


def get_player_info(conn, player_id):
    """
    Given player_id and DB conn, return player email address, first name, last name
    """

    with conn.cursor() as cur:
        select_statement = "SELECT `email`, `first_name`, `first_name` FROM Players WHERE `player_id` = %s"
        logger.debug("get_player_info(): {}".format(select_statement))
        cur.execute(select_statement, (player_id,))
        row = cur.fetchone()
        if row is None:
            return (None, None, None)
        else:
            (email, first_name, last_name) = row
            return (email, first_name, last_name)


def get_all_current_players(conn):
    """
    Return all rows in LOTW Players database
    who are registered to play in current season
    """
    year = get_current_year()
    with conn.cursor() as cur:
        select_statement = "SELECT `player_id`, `email`, `last_name`, `first_name`, `past_titles`, `rookie` FROM Players WHERE `" + str(year) + "_registration` = 1"
        cur.execute(select_statement)
        result = cur.fetchall()
        return result

def get_all_paid_players(conn):
    """
    Return all rows in LOTW Players database
    who have registered and paid in current season
    """
    year = get_current_year()
    with conn.cursor() as cur:
        select_statement = "SELECT `player_id`, `email`, `last_name`, `first_name`, `past_titles`, `rookie` FROM Players WHERE `" + str(year) + "_registration` = 1 AND `" + str(year) + "_paid` = 1"
        cur.execute(select_statement)
        result = cur.fetchall()
        return result

def get_all_players(conn):
    """
    Return all rows in LOTW Players database and current year's registration status
    """
    year = get_current_year()
    with conn.cursor() as cur:
        select_statement = "SELECT `player_id`, `email`, `last_name`, `first_name`, `past_titles`, `rookie`, `" + str(year) + "_registration` FROM Players"
        cur.execute(select_statement)
        result = cur.fetchall()
        return result

def get_past_registered_players(conn, year):
    """
    Return all rows in LOTW Players database and current year's registration status
    who were registered last year
    """
    reg_year = int(year)
    if reg_year < 2000 or reg_year > 2050:
        logger.error("get_registered_players(): invalid input year {}".format(year))
        raise

    cur_year = get_current_year()
    with conn.cursor() as cur:
        select_statement = "SELECT `player_id`, `email`, `last_name`, `first_name`, `past_titles`, `rookie`, `" + str(cur_year) + "_registration` FROM Players WHERE `" + str(reg_year) + "_registration` = 1 OR `rookie` = 1"
        cur.execute(select_statement)
        result = cur.fetchall()
        return result

def get_player_reg(conn, player_id):
    """
    Return row of one player in LOTW Players database including 
    current year's registration status
    """

    year = get_current_year()
    with conn.cursor() as cur:
        select_statement = "SELECT `player_id`, `email`, `last_name`, `first_name`, `past_titles`, `rookie`, `" + str(year) + "_registration` FROM Players WHERE `player_id` = %s"
        cur.execute(select_statement, (player_id,))
        result = cur.fetchall()
        return result


def get_player(conn, player_id):
    """
    Return row of one player in LOTW Players database
    """

    with conn.cursor() as cur:
        select_statement = "SELECT `player_id`, `email`, `last_name`, `first_name`, `past_titles`, `rookie` FROM Players WHERE `player_id` = %s"
        cur.execute(select_statement, (player_id,))
        result = cur.fetchall()
        return result



def get_player_by_email(conn, email):
    """
    Return row of one player in LOTW Players database
    """

    with conn.cursor() as cur:
        select_statement = "SELECT `player_id`, `email`, `last_name`, `first_name`, `past_titles`, `rookie` FROM Players WHERE `email` = %s"
        cur.execute(select_statement, (email,))
        result = cur.fetchall()
        return result


def get_current_year():
    """
    Determine current year
    Year is the year at the start of the NFL season
    If month is Jan, Feb, or Mar, return past year
    """
    time_now = datetime.datetime.now()
    month = time_now.month
    year = time_now.year
    if month == 1 or month == 2 or month == 3:
        year = year - 1
    logger.debug("get_current_year(): year is {}".format(year))
    return year


def get_current_week(conn):
    """
    Determine current week, return week as integer, 
    return None if week cannot be determined 
    """

    time_now = datetime.datetime.now()
    # Mon is 0, Tue 1, Wed 2, Thu 3, Fri 4, Sat 5, Sun 6
    weekday = time_now.weekday()

    offset_days = weekday - 2
    # Monday
    if offset_days == -2:
        offset_days = 5
    # Tuesday
    elif offset_days == -1:
        offset_days = 6

    # 0 Wed, 1 Thu, 2 Fri, 3 Sat, 4 Sun, 5 Mon, 6 Tue
    # results in Wed to Wed week

    offset_hours = time_now.hour
    offset_minutes = time_now.minute
    offset_seconds = time_now.second
    offset_microseconds = time_now.microsecond

    week_start_et = time_now - datetime.timedelta(days=offset_days, 
                                                hours=offset_hours,
                                                minutes=offset_minutes,
                                                seconds=offset_seconds,
                                                microseconds=offset_microseconds)


    # +4 to account for US/Eastern vs UTC timezone
    week_start = week_start_et + datetime.timedelta(hours=4)
    week_end = week_start + datetime.timedelta(days=7)
    logger.debug("get_current_week(): week_start {} week_end {}".format(week_start, week_end))

    with conn.cursor() as cur:
        select_statement = "SELECT `week` FROM Games_" + str(get_current_year()) + " WHERE `kickoff_time` >= %s AND `kickoff_time` < %s"
        logger.debug("week start {} week end {}".format(week_start, week_end))
        logger.debug("SQL: {}".format(select_statement))
        cur.execute(select_statement, (week_start, week_end))
        row = cur.fetchone()
        weeks = {}
        while row is not None:
            (week,) = row
            weeks[week] = 1
            row = cur.fetchone()

        if len(weeks) == 1:
            keys = list(weeks.keys())
            return keys[0]
        else:
            logger.debug("Unexpected number of weeks returned for current week {}".format(len(weeks)))

    return None

def in_daylight_savings():
    """
    Return True if current time is in daylight savings time window
    False otherwise
    """

    est = pytz.timezone("US/Eastern")
    td = est.localize(datetime.datetime.now()).dst()
    # total_seconds value of 3600 means in daylight savings
    if td.total_seconds() > 0:
        return True
    else:
        return False


def datetime_to_string(dt):
    """
    Given a datetime object stored in UTC, return a nicely formatted 
    string representation of the date/time in US/Eastern time.

    DAY MM/DD HH:MM a/p 
    """

    # convert datetime object from UTC to US/Eastern time
    from_zone = tz.tzutc()
    to_zone = tz.gettz('US/Eastern')
    utc = dt.replace(tzinfo=from_zone)
    et = utc.astimezone(to_zone)
    logger.debug("datetime_to_string(): converted {} UTC to {} US/Eastern".format(dt, et))

    et_string = et.strftime('%a %m/%d %I:%M %p')
    return et_string


def check_auth_token(conn, token, player_id, week):
    """
    check authentication token

    returns (authenticated, message)
    authenticated - boolean, true for successful auth and false otherwise
    message - string, success or error message, string
    """

    stored_token = get_auth_token(conn, player_id, week)

    # we always expect one token / player_id / week combination
    if stored_token is None:
        logger.error("no valid auth token found for player_id {} week {}".format(player_id, week))
        return (False, "no valid auth token found")

    # check token against token passed in by user
    if token == stored_token:
        return (True, "authentication successful for player_id {} week {} token {}".format(player_id, week, token))
    else:
        return (False, "invalid token")



def get_auth_token(conn, player_id, week):
    """
    get authentication token for player / week combination
    """

    with conn.cursor() as cur:
        select_statement = "SELECT `token` FROM Auth_Tokens WHERE `player_id` = %s AND `week` = %s"
        logger.debug("get_auth_token(): {}".format(select_statement))
        cur.execute(select_statement, (player_id, week))
        row = cur.fetchone()
        if row is not None:
            (token,) = row
            return token
        else:
            return None


def create_auth_token(conn, player_id, week):
    """
    create authentication token for player / week combination
    """

    stored_token = get_auth_token(conn, player_id, week)
    if stored_token is not None:
        logger.error("token already exists for player_id {} week {}".format(player_id, week))
        raise

    new_token = generate_auth_token()
    expiration_time = get_expiration_time(conn, week)

    # update database with token for player_id and week
    try:
        with conn.cursor() as cur:
            sql = "INSERT INTO `Auth_Tokens` (`token`, `player_id`, `week`, `expiration_time`) VALUES (%s, %s, %s, %s)"
            logger.debug("create_auth_token(): ".format(sql))
            cur.execute(sql, (new_token, player_id, week, expiration_time))
            conn.commit()
    except Exception as e:
        logger.error("Error updating database: {}".format(str(e)))
        raise

    return new_token


def generate_auth_token():
    """
    create random 8 character auth token string [a-zA-Z0-9]

    returns 8 char string
    """
    chars = 8
    return ''.join(random.choice(string.ascii_letters + string.digits) for m in range(chars))


def get_expiration_time(conn, week):
    """
    get kickoff time of last game in week for auth token expiration

    returns datetime object
    """

    with conn.cursor() as cur:
        select_statement = "SELECT `kickoff_time` FROM Games_" + str(get_current_year()) + " WHERE `week` = %s ORDER BY kickoff_time DESC"
        logger.debug("get_auth_token(): {}".format(select_statement))
        cur.execute(select_statement, week)
        row = cur.fetchone()
        if row is not None:
            (kickoff_time,) = row
            return kickoff_time
        else:
            return None


def get_team_name(conn, team_id):
    """
    Given team_id, return string of team name (City Nickname)
    """

    with conn.cursor() as cur:
        select_statement = "SELECT `city`, `nickname` FROM Teams WHERE `team_id` = %s"
        logger.debug("get_team_name(): {}".format(select_statement))
        cur.execute(select_statement, (team_id,))
        row = cur.fetchone()
        if row is not None:
            (city, nickname) = row
            return "{} {}".format(city, nickname)
        else:
            return None


def get_all_games(conn, week):
    """
    Get all games for current week

    return list of tuples:
    (kickoff_time, away_team_id, home_team_id, home_team_line)
    """
    with conn.cursor() as cur:
        select_statement = "SELECT * FROM Games_" + str(get_current_year()) + " WHERE `week` = %s"
        cur.execute(select_statement, (week, ))
        result = cur.fetchall()
        return result


def update_game_ats(conn, week):
    """
    Confirm all games in given week are complete and final scores recorded
    Update game Away and Home team ATS points in Games table

    returns tuple: (boolean success/failure, string message)
    """

    if week == 0:
        logger.info("No updates made for week 0")
        return(True, "No updates made for week 0")

    all_games = get_all_games(conn, week)
    for row in all_games:
        (game_id, week, kickoff_time, away_team_id, home_team_id, home_team_line, away_team_score, home_team_score, away_team_ats, home_team_ats) = row
        if away_team_score is None:
            return (False, "NULL value for away team score for game {}".format(game_id))

        if home_team_score is None:
            return (False, "NULL value for home team score for game {}".format(game_id))

        away_team_line = -(home_team_line)
        home_ats = home_team_line + (home_team_score - away_team_score)
        away_ats = away_team_line + (away_team_score - home_team_score)

        # update game ATS for each team
        try:
            with conn.cursor() as cur:
                sql = "UPDATE `Games_" + str(get_current_year()) + "` SET `away_team_ats` = %s, `home_team_ats` = %s WHERE `game_id` = %s"
                logger.debug("update_game_ats(): ".format(sql))
                cur.execute(sql, (away_ats, home_ats, game_id))
                conn.commit()
        except Exception as e:
            logger.error("Error updating database: {}".format(str(e)))
            raise

        logger.info("Successfully updated home and away ATS values for game {}".format(game_id)) 

    return (True, "Successfully updated game ATS values for week {}".format(week))



def update_pick_ats(conn, week):
    """
    Update pick_ats column in Picks table for given week

    returns tuple: (boolean success/failure, string message)
    """

    current_week = get_current_week(conn)
    if current_week is not None:
        # final week is 22, allow standings run final week
        if week >= current_week and week < 22:
            error_msg = "Skipping update, week to update {} is greater than or equal to current week {}".format(week, current_week)
            logger.error(error_msg)
            return (False, error_msg)

        if week == 0:
            error_msg = "Skipping update, standings week 0"
            logger.error(error_msg)
            return (False, error_msg)

    with conn.cursor() as cur:
        select_statement = "SELECT `pick_id`, `pick` FROM `Picks_" + str(get_current_year()) + "` WHERE `lock_in_time` IS NOT NULL AND `week` = %s" # AND `pick_ats` IS NULL"
        cur.execute(select_statement, (week, ))
        valid_picks = cur.fetchall()

        select_statement = "SELECT `away_team_id`, `home_team_id`, `away_team_ats`, `home_team_ats` FROM `Games_" + str(get_current_year()) + "` WHERE `week` = %s"
        cur.execute(select_statement, (week, ))
        all_games = cur.fetchall()

        # build hash of team => ATS result for given week
        team_results = {}
        for game in all_games:
            (away_team_id, home_team_id, away_team_ats, home_team_ats) = game
            if away_team_ats is None:
                error_msg = "Unexpected NULL away team ATS value for team {}".format(away_team_id)
                logger.error(error_msg)
                return (False, error_msg)
            if home_team_ats is None:
                error_msg = "Unexpected NULL home team ATS value for team {}".format(home_team_id)
                logger.error(error_msg)
                return (False, error_msg)

            team_results[away_team_id] = away_team_ats
            team_results[home_team_id] = home_team_ats

        # iterate through picks and update ATS
        for pick in valid_picks:
            (pick_id, team_id) = pick

            # find ATS result for pick
            pick_ats = team_results.get(team_id)

            if pick_ats is None:
                error_msg = "Unexpected null value for pick_ats for pick_id {}".format(pick_id)
                logger.error(error_msg)
                return (False, error_msg)

            try:
                with conn.cursor() as cur:
                    sql = "UPDATE `Picks_" + str(get_current_year()) + "` SET `pick_ats` = %s WHERE `pick_id` = %s"
                    logger.debug("update_pick_ats(): ".format(sql))
                    cur.execute(sql, (pick_ats, pick_id))
                    conn.commit()
            except Exception as e:
                logger.error("Error updating database: {}".format(str(e)))
                raise

            logger.info("Successfully updated ATS value for pick {}".format(pick_id)) 

    return (True, "Successfully updated pick ATS values for week {}".format(week))



def get_standings_message(conn, week):
    """
    Get weekly commissioner standings message string
    """
    with conn.cursor() as cur:
        select_statement = "SELECT `message` FROM `Standings_Message_" + str(get_current_year()) + "` WHERE `week` = %s"
        cur.execute(select_statement, (week, ))
        result = cur.fetchone()
        if result is None:
            return None
        else:
            return result[0]


def get_commish_message(conn, message_id):
    """
    Get commissioner message and e-mail subject
    """
    with conn.cursor() as cur:
        select_statement = "SELECT subject, message FROM Commissioner_Message WHERE message_id = %s"
        cur.execute(select_statement, (message_id, ))
        result = cur.fetchone()
        if result is None:
            return None
        else:
            return result


def get_all_player_picks(conn, player_id, week):
    """
    For given player get all valid picks up to week

    returns list of tuples containing
    (week, pick, pick_ats)
    """
    with conn.cursor() as cur:
        select_statement = "SELECT `week`, `pick`, `pick_ats` FROM `Picks_" + str(get_current_year()) + "` WHERE `player_id` = %s AND `week` <= %s and `lock_in_time` IS NOT NULL"
        cur.execute(select_statement, (player_id, week))
        result = cur.fetchall()
        return result



def get_all_picks(conn, week):
    """
    For given week get all valid picks

    returns list of tuples containing
    (player_id, pick, pick_ats)
    """
    with conn.cursor() as cur:
        select_statement = "SELECT `player_id`, `pick`, `pick_ats` FROM `Picks_" + str(get_current_year()) + "` WHERE `week` = %s and `lock_in_time` IS NOT NULL"
        cur.execute(select_statement, (week, ))
        result = cur.fetchall()
        return result



def get_standings(conn):
    """
    Return LOTW standings with player names and attributes

    returns list of tuples of format:
    (player_id, last_name, first_name, past_titles, rookie (boolean), wins, losses, win_percentage, ats_points)
    """

    with conn.cursor() as cur:
        select_statement = "SELECT Standings_" + str(get_current_year()) + ".player_id, `last_name`, `first_name`, `past_titles`, `rookie`, `wins`, `losses`, `win_percentage`, `ats_points` FROM Standings_" + str(get_current_year()) + " INNER JOIN Players ON Standings_" + str(get_current_year()) + ".player_id = Players.player_id ORDER BY win_percentage DESC, ats_points DESC, last_name ASC, first_name ASC"
        logger.debug("get_standings(): ".format(select_statement))
        cur.execute(select_statement)
        rows = cur.fetchall()
        return rows        


def get_standings_full_name(first_name, last_name, past_titles, rookie):
    """
    given first_name, last_name, past_titles (int), rookie (boolean)
    return string representing full name with past title and rookie symbols
    """
    full_name = last_name + ", " + first_name
    logger.debug("Building full name {} {} {} {}".format(first_name, last_name, past_titles, rookie))
    if rookie:
        full_name += " &reg;"
        return full_name

    i = 0
    while i < past_titles:
        full_name += " &copy;"
        i += 1
    return full_name


def build_lines_html_head():
    """
    Build lines html head
    """

    html = """
<html>
<head>
  <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css">
    <style>
        .inline {
          display: inline;
        }

        .message {
          display: inline;
          font-size: 2em;
        }

h1 {
    font-size: 2.5rem;
}

h2 {
    font-size: 2.25rem;
}

h3 {
    font-size: 2rem;
}

h4 {
    font-size: 1.75rem;
}

h5 {
    font-size: 1.5rem;
}

h6 {
    font-size: 1.25rem;
}

p {
    font-size: 1rem;
}

@media (max-width: 480px) {
    html {
        font-size: 12px;
    }
}

@media (min-width: 480px) {
    html {
        font-size: 13px;
    }
}

@media (min-width: 768px) {
    html {
        font-size: 14px;
    }
}

@media (min-width: 992px) {
    html {
        font-size: 15px;
    }
}

@media (min-width: 1200px) {
    html {
        font-size: 16px;
    }
}

        body {
            margin: 20;
            font-family: "Arial", "Helvetica", sans-serif;
        }
        table {
            border-collapse: collapse;
            border: 1px solid black;
        }
        th {
            border: 1px solid black;
            padding: 6px;
            text-align: left;
            background-color: lightgrey;
        }
        td {
            border: 1px solid black;
            padding: 6px;
            text-align: left;
        }
     </style>
</head>
"""
    return html



def build_lines_html_body(conn, player_id, week):
    """
    Given database connection and current week, return body of
    LOTW line email without the html/body tags
    """
    html = "<h4>LOTW: WEEK {} LINES</h4>\n".format(week)
    html = html + """
<table>
<tr>
    <th>Kickoff Time</th>
    <th>Away Team</th>
    <th>Home Team</th>
    <th>Line</th>
</tr>
"""

    # read games for week and populate table
    with conn.cursor() as cur:
        select_statement = "SELECT `kickoff_time`, `away_team_id`, `home_team_id`, `home_team_line` FROM Games_" + str(get_current_year()) + " WHERE `week` = %s"
        cur.execute(select_statement, (week,))
        rows = cur.fetchall()
        for row in rows:
            (kickoff_time, away_team_id, home_team_id, home_team_line) = row
            logger.debug("build_lines_html_body(): processing row {} {} {} {}".format(kickoff_time, away_team_id, home_team_id, home_team_line))
            html_row = build_lines_table_row(conn, player_id, week, kickoff_time, away_team_id, home_team_id, home_team_line)
            html = html + html_row
    
    html = html + "</table></body></html>"
    return html


