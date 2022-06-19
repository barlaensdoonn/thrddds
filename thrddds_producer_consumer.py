# references:
# https://realpython.com/python-deque/#sharing-data-between-threads
# https://alexandra-zaharia.github.io/posts/how-to-stop-a-python-thread-cleanly/#by-default-the-thread-is-not-stopped-cleanly
# updated: 2022-06-18

import os
import time
import signal
import random
import logging
import logging.config
import threading
from collections import deque
from datetime import datetime, timedelta


# CONFIGURE LOGGING
log_level = 'INFO'
log_filename = 'thrddds_prohdsumption.log'
log_file = os.path.join('logs', log_filename)
logging_config = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s - %(levelname)s - %(name)s - %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        },
    },
    'handlers': {
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'maxBytes': 10485760,
            'backupCount': 2,
            'level': log_level,
            'formatter': 'standard',
            'filename': log_file,
            'encoding': 'utf8'
        },
        'stream': {
            'class': 'logging.StreamHandler',
            'level': log_level,
            'formatter': 'standard',
            'stream': 'ext://sys.stdout'
        }
    },
    'loggers': {
        '': {
            'handlers': ['file', 'stream'],
            'level': log_level,
            'propagate': False
        }
    }
}
logging.config.dictConfig(logging_config)
logger = logging.getLogger('thrddds_prohdsumption')
logger.info('- - - - - - - - - - - - - - - - -')
logger.info('logging configured')


def wait(mins=0.3, maxs=0.9):
    time.sleep(mins + random.random() * (maxs - mins))


def produce(queue, size):
    plogger = logging.getLogger('prohd')
    plogger.info('hi sumption')
    wait()
    while not stop_event.is_set():
        if len(queue) < size:
            value = random.randint(0, 9)
            queue.append(value)
            plogger.info(f'pushed {value} to the queue')
        else:
            plogger.info('queue full')
        wait(0.1, 0.5)
    plogger.info("i hear there's good empirical evidence that suggests a 4-day work week doesn't impact company performance")


def consume(queue):
    clogger = logging.getLogger('sumption')
    clogger.info('hi prohd')
    wait()
    while not stop_event.is_set():
        try:
            value = queue.popleft()
            clogger.info(f'popped {value} off the queue')
        except IndexError:
            clogger.info('nothing pending on the queue')
        else:
            clogger.info(f'{len(queue)} items pending on the queue')
        wait(0.2, 0.7)
    clogger.info('i have lunch at the same time everyday')


def handle_kb_interrupt(sig, frame):
    '''
    intercept KeyboardInterrupt and set threading.Event, which causes threads
    to exit and shutdown gracefully. stop_event is initialized in main thread.
    '''
    logger.info('shutting down early today')
    stop_event.set()


if __name__ == '__main__':
    # register handle_kb_interrupt with SIGINT to be able to shutdown threads with ^C
    stop_event = threading.Event()
    signal.signal(signal.SIGINT, handle_kb_interrupt)

    # threads and their shared "queue"
    shared = deque()
    prohd = threading.Thread(target=produce, args=(shared, 10))
    sump = threading.Thread(target=consume, args=(shared,))

    # set a time in the future to exit the program
    shift_duration = timedelta(seconds=60)
    logger.info(f"starting a threaded work shift of duration: {int(shift_duration.total_seconds())} seconds")
    time.sleep(1)

    # start the threads
    logger.info("threads, i'll leave you to it")
    time.sleep(2)
    breaktime = datetime.now() + shift_duration
    prohd.start()
    sump.start()

    # wait until scheduled break time
    # don't check the time too often, so main thread doesn't eat up CPU and cycles
    while breaktime > datetime.now():
        time.sleep(0.5)

    # execute scheduled stop by setting the threading.Event()
    logger.info('shift over')
    logger.info("threads, let's get out of here")
    stop_event.set()
    time.sleep(1)
    prohd.join()
    sump.join()
    logger.info('bye')
