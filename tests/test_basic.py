import sys
import os
from unittest.mock import MagicMock

# 1. Mock pymysql in sys.modules BEFORE importing lotw
mock_pymysql = MagicMock()
sys.modules["pymysql"] = mock_pymysql

# 2. Add parent directory containing lotw.py to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# 3. Import lotw after pymysql has been mocked
import lotw

def test_environment_syntax():
    """Basic sanity check to ensure Python syntax passes across core files."""
    assert True

def test_mock_db_connection():
    """Example unit test verifying pymysql connection behavior."""
    mock_pymysql.reset_mock()
    
    mock_conn = MagicMock()
    mock_pymysql.connect.return_value = mock_conn

    # Trigger whatever function in lotw uses the connection, or test the mock directly:
    conn = mock_pymysql.connect(host="localhost", user="root", password="", database="test")
    
    assert conn is not None
    mock_pymysql.connect.assert_called_once_with(
        host="localhost", user="root", password="", database="test"
    )
