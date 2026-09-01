#!/usr/bin/env python3

import os
import sys
import datetime
from dateutil import tz

# Source data found at https://www.pro-football-reference.com/years/

def get_kickoff_time(date, time):
    """
    Given a date and time string in US/Eastern time, return datetime object in UTC
    """
    m_dict = {
        'January': 1,
        'February': 2,
        'March': 3,
        'April': 4,
        'May': 5,
        'June': 6,
        'July': 7,
        'August': 8,
        'September': 9,
        'October': 10,
        'November': 11,
        'December': 12
    }

    year = int(2026)
    (timestring, ap) = time.split(' ')
    (month, day) = date.split(' ')
    if int(m_dict[month]) <= 3:
        year = year + 1
    (hour, minute) = timestring.split(':')
    if ap == 'PM' and int(hour) < 12:
        hour = int(hour) + 12
    eastern_time = tz.gettz('US/Eastern')
    utc = tz.gettz('UTC')

    kickoff_time = datetime.datetime(year, m_dict[month], int(day), hour=int(hour), minute=int(minute), second=int(0), tzinfo=eastern_time) 
    return kickoff_time.astimezone(utc)


def get_team_id(team):
    """
    Given full team name as string, ex: "Detroit Lions", return LOTW team_id
    """

    team_id_dict = {
        'Arizona Cardinals': 'ARI',
        'Atlanta Falcons': 'ATL',
        'Baltimore Ravens': 'BAL',
        'Buffalo Bills': 'BUF',
        'Carolina Panthers': 'CAR',
        'Chicago Bears': 'CHI',
        'Cincinnati Bengals': 'CIN',
        'Cleveland Browns': 'CLE',
        'Dallas Cowboys': 'DAL',
        'Denver Broncos': 'DEN',
        'Detroit Lions': 'DET',
        'Green Bay Packers': 'GNB',
        'Houston Texans': 'HOU',
        'Indianapolis Colts': 'IND',
        'Jacksonville Jaguars': 'JAX',
        'Kansas City Chiefs': 'KAN',
        'Los Angeles Chargers': 'LAC',
        'Los Angeles Rams': 'LAR',
        'Miami Dolphins': 'MIA',
        'Minnesota Vikings': 'MIN',
        'New Orleans Saints': 'NOR',
        'New England Patriots': 'NWE',
        'New York Giants': 'NYG',
        'New York Jets': 'NYJ',
        'Las Vegas Raiders': 'LVR',
        'Philadelphia Eagles': 'PHI',
        'Pittsburgh Steelers': 'PIT',
        'Seattle Seahawks': 'SEA',
        'San Francisco 49ers': 'SFO',
        'Tampa Bay Buccaneers': 'TAM',
        'Tennessee Titans': 'TEN',
        'Washington Commanders': 'WAS'
    }

    return team_id_dict.get(team)



schedule_file = open("schedule_2026.csv", "r")

# ==> schedule_2019.csv <==
#1,Thu,September 5,Green Bay Packers,,@,Chicago Bears,,8:20 PM
#1,Sun,September 8,Los Angeles Rams,,@,Carolina Panthers,,1:00 PM
#1,Sun,September 8,Tennessee Titans,,@,Cleveland Browns,,1:00 PM

for line in schedule_file:
    game = line.rstrip()
    (week, weekday, month_day, away, blank1, atsign, home, blank2, time) = game.split(',')

    kickoff_time = get_kickoff_time(month_day, time)
    away_team_id = get_team_id(away)
    home_team_id = get_team_id(home)

    if kickoff_time is None:
        print("Error computing kickoff_time")
        sys.exit()

    if away_team_id is None:
        print("Error computing away_team_id")
        sys.exit()

    if home_team_id is None:
        print("Error computing home_team_id")
        sys.exit()

    sql = "INSERT INTO Games_2026 (`week`, `kickoff_time`, `away_team_id`, `home_team_id`) "
    sql += "VALUES ('{}', '{}', '{}', '{}');".format(week, kickoff_time, away_team_id, home_team_id)

    print(sql)


schedule_file.close()
sys.exit()

