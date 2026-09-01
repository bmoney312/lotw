import unittest
from unittest.mock import patch, MagicMock
import os
import json

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
import update_standings
import emit_lotw_metrics


class TestAPIProcessing(unittest.TestCase):
    
    @patch('process_pick.pymysql.connect')
    @patch('process_pick.validate_field')
    @patch('process_pick.submit_pick')
    @patch('process_pick.send_email')
    @patch('process_pick.get_player_info')
    def test_process_pick_success(self, mock_get_player_info, mock_send_email, mock_submit_pick, mock_validate, mock_connect):
        # 1. Setup Mocks
        mock_connect.return_value = MagicMock()
        mock_validate.return_value = True
        mock_submit_pick.return_value = (True, "SEA", -3, "Your pick was updated successfully!")
        mock_get_player_info.return_value = ("test@example.com", "John", "Doe")
        mock_send_email.return_value = True
        
        # 2. Define API Gateway Payload
        event = {
            "body": "pick=SEA&week=1&player_id=123"
        }
        
        # 3. Execute Handler
        response = process_pick.lambda_handler(event, {})
        
        # 4. Assertions
        self.assertEqual(response['statusCode'], 200)
        self.assertIn("Your pick was updated successfully!", response['body'])
        mock_submit_pick.assert_called_once()
        mock_send_email.assert_called_once()

    @patch('process_pick.pymysql.connect')
    def test_process_pick_missing_body(self, mock_connect):
        event = {} # Missing body
        response = process_pick.lambda_handler(event, {})
        
        self.assertEqual(response['statusCode'], 400)
        self.assertIn("Bad Request [body]", response['body'])


class TestEmailGeneration(unittest.TestCase):

    @patch('email_picks.pymysql.connect')
    @patch('email_picks.smtp_connect')
    @patch('email_picks.smtp_send')
    @patch('email_picks.get_all_paid_players')
    @patch('email_picks.get_standings')
    @patch('email_picks.get_picks_at_kickoff_time')
    @patch('email_picks.cloudwatch') # Patch the instantiated object directly
    def test_email_picks_scheduled_event(self, mock_cloudwatch, mock_get_picks, mock_get_standings, mock_get_players, mock_smtp_send, mock_smtp_conn, mock_db_conn):
        # 1. Setup Mocks
        mock_db_conn.return_value = MagicMock()
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


class TestDatabaseUpdates(unittest.TestCase):

    @patch('update_standings.pymysql.connect')
    @patch('update_standings.update_game_ats')
    @patch('update_standings.update_pick_ats')
    @patch('update_standings.update_standings_table')
    def test_update_standings_manual_run(self, mock_update_table, mock_update_pick_ats, mock_update_game_ats, mock_connect):
        # 1. Setup Mocks
        mock_connect.return_value = MagicMock()
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

    @patch('emit_lotw_metrics.pymysql.connect')
    @patch('emit_lotw_metrics.boto3.client')
    @patch('emit_lotw_metrics.get_all_current_players')
    @patch('emit_lotw_metrics.get_all_picks')
    def test_emit_metrics(self, mock_get_picks, mock_get_players, mock_boto_client, mock_connect):
        # 1. Setup DB Mock
        mock_db = MagicMock()
        mock_connect.return_value = mock_db
        
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
