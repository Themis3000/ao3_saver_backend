import time


def heartbeat_loop():
    while True:
        time.sleep(60)
        do_heartbeat()


def do_heartbeat():
    # Do db maintenance tasks
    pass
