### This code is property of the GGAO ###


# Native imports
import signal


class GracefulKiller:
    kill_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        """ When receive a SIGINT or SIGTERM signal. Wait until the current process ends to die. """
        self.kill_now = True
