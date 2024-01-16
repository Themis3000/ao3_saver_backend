import time

import db


def heartbeat_loop():
    while True:
        time.sleep(120)
        do_heartbeat()


def do_heartbeat():
    # Do db maintenance tasks
    db.clear_queue_by_attempts(3)
