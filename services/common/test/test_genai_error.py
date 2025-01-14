### This code is property of the GGAO ###

from errors.genaierrors import GenaiError 

def test_initialization():
    """Test that the GracefulKiller initializes and sets up signal handlers."""
    error = GenaiError(400, "Not Found")
    assert error.status_code == 400
    assert error.message == "Not Found"
