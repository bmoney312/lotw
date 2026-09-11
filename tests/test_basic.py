import unittest
from unittest.mock import patch, MagicMock
import os
import json
import datetime

# Set required environment variables before importing the Lambda handlers
# We include dummy AWS credentials to prevent boto3 from searching for real ones during module load
os.environ.update({
    'db_endpoint': 'mock-endpoint',
    'db_port': '3306',
    'db_username': 'mock-user',
    'db_password': 'mock-pass',
    'db_name': 'mock-db',
    'mail_username': 'mock-mail-user',
    'mail_password': 'mock-mail-pass',
    'mail_host': 'mock-mail-host',
    'mail_port': '587',
    'week': '1',
    'AWS_ACCESS_KEY_ID': 'testing',
    'AWS_SECRET_ACCESS_KEY': 'testing',
    'AWS_DEFAULT_REGION': 'us-west-2'
})

# Import your Lambda modules
import process_pick
import email_picks
import email_standings
import update_standings
import emit_lotw_metrics
import email_registration
import process_registration
import add_lotw_player
import email_analytics
import email_lines


class TestAPIProcessing(unittest.TestCase):

    @patch('process_pick.get_db_connection')
    @patch('process_pick.validate_field')
    @patch('process_pick.submit_pick')
    @patch('process_pick.send_email')
    @patch('process_pick.get_player_info')
    def test_process_pick_success(self, mock_get_player_info, mock_send_email, mock_submit_pick, mock_validate, mock_db_conn):
        # 1. Setup Mocks
        mock_conn = MagicMock()
        mock_db_conn.return_value = mock_conn
        mock_validate.return_value = True
        mock_submit_pick.return_value = (True, "SEA", -3, "Your pick was updated successfully!")
        mock_get_player_info.return_value = ("test@example.com", "John", "Doe")
        mock_send_email.return_value = True

        # 2. Define API Gateway Payload with valid human_click token
        event = {
            "body": "pick=SEA&week=1&player_id=123&user_action=human_click"
        }

        # 3. Execute Handler
        response = process_pick.lambda_handler(event, {})

        # 4. Assertions
        self.assertEqual(response['statusCode'], 200)
        self.assertIn("Your pick was updated successfully!", response['body'])
        mock_submit_pick.assert_called_once()
        mock_send_email.assert_called_once()

    @patch('process_pick.get_db_connection')
    def test_process_pick_missing_body(self, mock_db_conn):
        mock_conn = MagicMock()
        mock_db_conn.return_value = mock_conn

        event = {} # Missing body
        response = process_pick.lambda_handler(event, {})

        self.assertEqual(response['statusCode'], 400)
        self.assertIn("Bad Request [body]", response['body'])

    @patch('process_pick.get_db_connection')
    def test_process_pick_rejects_missing_user_action(self, mock_db_conn):
        mock_conn = MagicMock()
        mock_db_conn.return_value = mock_conn

        # Missing user_action token
        event = {
            "body": "pick=SEA&week=1&player_id=123"
        }
        response = process_pick.lambda_handler(event, {})

        self.assertEqual(response['statusCode'], 400)
        self.assertIn("Invalid submission", response['body'])


class TestEmailGeneration(unittest.TestCase):

    @patch('email_picks.get_db_connection')
    @patch('email_picks.smtp_connect')
    @patch('email_picks.smtp_send')
    @patch('email_picks.get_all_paid_players')
    @patch('email_picks.get_standings')
    @patch('email_picks.get_picks_at_kickoff_time')
    @patch('email_picks.cloudwatch') # Patch the instantiated object directly
    def test_email_picks_scheduled_event(self, mock_cloudwatch, mock_get_picks, mock_get_standings, mock_get_players, mock_smtp_send, mock_smtp_conn, mock_db_conn):
        # 1. Setup Mocks
        mock_conn = MagicMock()
        mock_db_conn.return_value = mock_conn
        mock_smtp = MagicMock()
        mock_smtp_conn.return_value = mock_smtp
        mock_smtp_send.return_value = True

        # Mocking player list (player_id, player_email, last_name, first_name, titles, is_rookie)
        mock_get_players.return_value = [
            (1, "p1@example.com", "Doe", "John", 0, 1),
            (2, "p2@example.com", "Smith", "Jane", 1, 0)
        ]
        mock_get_standings.return_value = []
        mock_get_picks.return_value = {1: ("SEA", -3), 2: ("DEN", 4)}

        # 2. Define EventBridge Payload
        event = {
            "detail-type": "Scheduled Event",
            "resources": ["arn:aws:events:us-west-2:123456789:rule/Scheduled_Picks"]
        }

        # 3. Execute Handler
        response = email_picks.lambda_handler(event, {})

        # 4. Assertions
        self.assertEqual(response['statusCode'], 200)
        self.assertEqual(mock_smtp_send.call_count, 2) # Emailed 2 players
        mock_cloudwatch.put_metric_data.assert_called_once() # Assert the method was called on the object

    @patch('email_standings.cloudwatch')
    @patch('email_standings.get_db_connection')
    @patch('email_standings.smtp_connect')
    @patch('email_standings.smtp_send')
    @patch('email_standings.get_all_paid_players')
    @patch('email_standings.get_standings')
    @patch('email_standings.get_current_pick')
    @patch('email_standings.get_standings_message')
    @patch('email_standings.get_player_season_details')
    def test_email_standings_scheduled_event(
        self,
        mock_season_details,
        mock_standings_msg,
        mock_current_pick,
        mock_get_standings,
        mock_get_players,
        mock_smtp_send,
        mock_smtp_connect,
        mock_db_conn,
        mock_cloudwatch
    ):
        # 1. Setup DB and SMTP mocks
        mock_conn = MagicMock()
        mock_db_conn.return_value = mock_conn

        mock_smtp = MagicMock()
        mock_smtp_connect.return_value = mock_smtp
        mock_smtp_send.return_value = True

        # 2. Setup mock data
        # Players: (player_id, email, last, first, titles, rookie)
        mock_get_players.return_value = [
            (1, "p1@example.com", "Doe", "John", 0, 1),
            (2, "p2@example.com", "Smith", "Jane", 1, 0)
        ]

        # Standings: (player_id, last_name, first_name, past_titles, rookie, wins, losses, win_percentage, ats_points, streak)
        mock_get_standings.return_value = [
            (1, "Doe", "John", 0, 1, 2, 0, 1.000, 10, "W2"),
            (2, "Smith", "Jane", 1, 0, 1, 1, 0.500, -2, "L1")
        ]

        # Current pick: (pick_id, pick, line, pick_ats, locked_in)
        mock_current_pick.return_value = (101, "SEA", -3, 7, True)
        mock_standings_msg.return_value = "Great week everyone!"
        mock_season_details.return_value = ([], 1, 0, 0)

        # 3. Invoke handler
        event = {"detail-type": "Scheduled Event"}
        response = email_standings.lambda_handler(event, {})

        # 4. Assertions
        self.assertEqual(response['statusCode'], 200)
        self.assertEqual(mock_smtp_send.call_count, 2)
        mock_cloudwatch.put_metric_data.assert_called_once()


class TestDatabaseUpdates(unittest.TestCase):

    @patch('update_standings.get_db_connection')
    @patch('update_standings.update_game_ats')
    @patch('update_standings.update_pick_ats')
    @patch('update_standings.update_standings_table')
    def test_update_standings_manual_run(self, mock_update_table, mock_update_pick_ats, mock_update_game_ats, mock_db_conn):
        # 1. Setup Mocks
        mock_conn = MagicMock()
        mock_db_conn.return_value = mock_conn
        mock_update_game_ats.return_value = (True, "Success")
        mock_update_pick_ats.return_value = (True, "Success")
        
        # 2. Define Manual Trigger Payload
        event = {
            "detail-type": "manual_run"
        }
        
        # 3. Execute Handler
        response = update_standings.lambda_handler(event, {})
        
        # 4. Assertions
        self.assertEqual(response['statusCode'], 200)
        mock_update_game_ats.assert_called_once()
        mock_update_pick_ats.assert_called_once()
        mock_update_table.assert_called_once()


class TestMetricsEmission(unittest.TestCase):

    @patch('emit_lotw_metrics.get_db_connection')
    @patch('emit_lotw_metrics.boto3.client')
    @patch('emit_lotw_metrics.get_all_current_players')
    @patch('emit_lotw_metrics.get_all_picks')
    def test_emit_metrics(self, mock_get_picks, mock_get_players, mock_boto_client, mock_db_conn):
        # 1. Setup DB Mock
        mock_db = MagicMock()
        mock_db_conn.return_value = mock_db
        
        # Mock DB Cursor behavior for the reg columns logic
        mock_cursor = MagicMock()
        mock_db.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [("2024_registration",), ("2025_registration",)]
        mock_cursor.fetchone.return_value = [10] # Mock count return
        
        # Setup Boto3 Mock
        mock_cloudwatch = MagicMock()
        mock_boto_client.return_value = mock_cloudwatch
        
        mock_get_players.return_value = [1, 2, 3] # 3 active players
        mock_get_picks.return_value = [1, 2] # 2 picks made
        
        event = {"detail-type": "Scheduled Event"}
        
        # 3. Execute Handler
        response = emit_lotw_metrics.lambda_handler(event, {})
        
        # 4. Assertions
        self.assertEqual(response['statusCode'], 200)
        self.assertTrue(mock_cloudwatch.put_metric_data.called)


class TestRegistrationFlow(unittest.TestCase):

    @patch('process_registration.get_db_connection')
    @patch('process_registration.validate_field')
    @patch('process_registration.submit_registration')
    @patch('process_registration.get_player_info')
    @patch('process_registration.send_email')
    def test_process_registration_success(self, mock_send_email, mock_get_player_info, mock_submit, mock_validate, mock_db_conn):
        # Setup mocks for receiving a player's opt-in choice
        mock_conn = MagicMock()
        mock_db_conn.return_value = mock_conn
        mock_validate.return_value = True
        mock_submit.return_value = (True, "Your registration was updated successfully!")
        mock_get_player_info.return_value = ("test@example.com", "John", "Doe")
        mock_send_email.return_value = True

        # Simulate an API Gateway GET request with query strings
        event = {
            "queryStringParameters": {
                "id": "123",
                "registration": "true"
            }
        }

        response = process_registration.lambda_handler(event, {})

        self.assertEqual(response['statusCode'], 200)
        self.assertIn("registration was updated successfully", response['body'])
        mock_submit.assert_called_once_with(mock_conn, 123, True, unittest.mock.ANY)
        mock_send_email.assert_called_once()

    @patch('email_registration.cloudwatch')
    @patch('email_registration.get_db_connection')
    @patch('email_registration.smtp_connect')
    @patch('email_registration.smtp_send')
    @patch('email_registration.get_past_registered_players')
    def test_email_registration_skips_registered(self, mock_get_players, mock_smtp_send, mock_smtp_connect, mock_db_conn, mock_cw):
        mock_conn = MagicMock()
        mock_db_conn.return_value = mock_conn

        mock_smtp = MagicMock()
        mock_smtp_connect.return_value = mock_smtp
        mock_smtp_send.return_value = True

        # Mock players: (player_id, email, last, first, titles, rookie, registered_status)
        # Player 2 has a status of 1 (already registered), so they should be skipped
        mock_get_players.return_value = [
            (1, "p1@example.com", "Doe", "John", 0, 1, None),
            (2, "p2@example.com", "Smith", "Jane", 1, 0, 1)
        ]

        event = {"detail-type": "Scheduled Event"}

        response = email_registration.lambda_handler(event, {})

        self.assertEqual(response['statusCode'], 200)
        self.assertEqual(mock_smtp_send.call_count, 1) # Ensure only player 1 receives an email

class TestPlayerManagement(unittest.TestCase):

    @patch('add_lotw_player.get_db_connection')
    @patch('add_lotw_player.add_lotw_player')
    @patch.dict(os.environ, {'email': 'new@example.com', 'first_name': 'New', 'last_name': 'Player'}, clear=False)
    def test_add_player_manual_run(self, mock_add_player, mock_db_conn):
        # Return a mock connection object with a close() method
        mock_conn = MagicMock()
        mock_db_conn.return_value = mock_conn
        mock_add_player.return_value = (True, "Successfully added player")

        event = {"detail-type": "manual_run"}
        response = add_lotw_player.lambda_handler(event, {})

        self.assertEqual(response['statusCode'], 200)
        mock_add_player.assert_called_once_with(mock_conn, 'new@example.com', 'New', 'Player', False)


class TestAnalyticsGeneration(unittest.TestCase):

    @patch('email_analytics.get_db_connection')
    @patch('email_analytics.smtp_connect')
    @patch('email_analytics.smtp_send')
    @patch('email_analytics.get_all_paid_players')
    @patch('email_analytics.get_team_ats_records')
    @patch('email_analytics.get_all_career_standings')
    @patch('email_analytics.get_standings')
    @patch('email_analytics.get_player_season_details')
    @patch('email_analytics.get_player_career_stats')
    @patch('email_analytics.get_player_yearly_history')
    @patch('email_analytics.cloudwatch')
    def test_email_analytics(self, mock_cloudwatch, mock_yearly, mock_career, mock_season, mock_standings, mock_career_standings, mock_team_ats, mock_get_players, mock_smtp_send, mock_smtp_connect, mock_db_conn):
        # Mocking DB connection
        mock_conn = MagicMock()
        mock_db_conn.return_value = mock_conn

        # Mocking SMTP
        mock_smtp = MagicMock()
        mock_smtp_connect.return_value = mock_smtp
        mock_smtp_send.return_value = True

        # Mocking deep SQL analytics dependencies
        mock_get_players.return_value = [(1, "p1@example.com", "Doe", "John", 0, 1)]
        mock_team_ats.return_value = [("Seahawks", 1, 0, 1.0, 1, 0, 0, 0)]
        mock_career_standings.return_value = [(1, "Doe, John", 10, 5, 0.66)]
        mock_standings.return_value = [(1, "Doe", "John", 0, 1, 10, 5, 0.66, 15, "W2")]
        mock_season.return_value = ([{'week': 1, 'pick': 'SEA -3', 'game_result': 'SEA 20 v DEN 10', 'site': 'Home', 'result': 'Win', 'type': 'Favorite'}], 1, 0, 0)
        mock_career.return_value = (10, 5, 8, 4, 3)
        mock_yearly.return_value = [{'year': 2025, 'w': 10, 'l': 5}]

        event = {"detail-type": "Scheduled Event"}

        response = email_analytics.lambda_handler(event, {})

        self.assertEqual(response['statusCode'], 200)
        mock_smtp_send.assert_called_once()
        mock_cloudwatch.put_metric_data.assert_called_once()


class TestWeeklyDistributions(unittest.TestCase):

    @patch('email_lines.get_db_connection')
    @patch('email_lines.smtp_connect')
    @patch('email_lines.smtp_send')
    @patch('email_lines.get_all_paid_players')
    @patch('email_lines.create_auth_token')
    @patch('email_lines.get_current_pick')
    @patch('email_lines.build_lines_email_body')
    @patch('email_lines.cloudwatch')
    def test_email_lines_uses_existing_token(self, mock_cloudwatch, mock_build_body, mock_get_pick, mock_create_token, mock_get_players, mock_smtp_send, mock_smtp_connect, mock_db_conn):
        # 1. Mock DB connection and cursor for batch pre-fetching
        mock_conn = MagicMock()
        mock_db_conn.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Mock cursor fetchall calls in order:
        # Call 1: Teams map -> [(team_id, team_name)]
        # Call 2: Games for week -> [(kickoff_time, away_team_id, home_team_id, home_team_line)]
        # Call 3: Existing auth tokens -> [(player_id, token)]
        mock_cursor.fetchall.side_effect = [
            [("SEA", "Seattle Seahawks")],                                    # Teams
            [(datetime.datetime(2026, 9, 13, 17, 0), "SEA", "DEN", -3)],     # Games
            [(1, "ABC12345")]                                                 # Auth tokens (Existing token for player 1)
        ]

        # 2. Mock SMTP
        mock_smtp = MagicMock()
        mock_smtp_connect.return_value = mock_smtp
        mock_smtp_send.return_value = True

        # 3. Mock player and pick details
        mock_get_players.return_value = [(1, "p1@example.com", "Doe", "John", 0, 1)]
        mock_get_pick.return_value = (None, "NOP", None, None, False)  # Pick not locked
        mock_build_body.return_value = "Mock body"

        event = {"detail-type": "Scheduled Event"}

        response = email_lines.lambda_handler(event, {})

        # 4. Assertions
        self.assertEqual(response['statusCode'], 200)
        mock_smtp_send.assert_called_once()
        mock_cloudwatch.put_metric_data.assert_called_once()
        mock_create_token.assert_not_called()  # Verified: existing token used, no new token minted
