### This code is property of the GGAO ###

import signal
from unittest.mock import patch
from graceful_killer import GracefulKiller 

def test_initialization():
    """Test that the GracefulKiller initializes and sets up signal handlers."""
    with patch("signal.signal") as mock_signal:
        killer = GracefulKiller()
        assert not killer.kill_now
        mock_signal.assert_any_call(signal.SIGINT, killer.exit_gracefully)
        mock_signal.assert_any_call(signal.SIGTERM, killer.exit_gracefully)

def test_exit_gracefully():
    """Test the exit_gracefully method sets kill_now to True."""
    killer = GracefulKiller()
    assert not killer.kill_now  # Initial state

    killer.exit_gracefully()  # Simulate a signal
    assert killer.kill_now  # Ensure kill_now is set to True

def test_signal_handling():
    """Test that signals trigger the exit_gracefully method."""
    killer = GracefulKiller()

    with patch.object(killer, "exit_gracefully") as mock_exit:
        signal.raise_signal(signal.SIGINT)  # Simulate SIGINT

        mock_exit.reset_mock()
        signal.raise_signal(signal.SIGTERM)  # Simulate SIGTERM

