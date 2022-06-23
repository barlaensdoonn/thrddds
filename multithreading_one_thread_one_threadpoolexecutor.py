'''
references:
  - https://docs.python.org/3/library/concurrent.futures.html#
  - https://superfastpython.com/threadpoolexecutor-in-python/
  - https://stackoverflow.com/a/58829816
  - https://realpython.com/python-deque/#sharing-data-between-threads
  - https://alexandra-zaharia.github.io/posts/how-to-stop-a-python-thread-cleanly/#by-default-the-thread-is-not-stopped-cleanly
  - https://gist.github.com/magnetikonline/a26ae80e2e23fcfda5b03ccb470f79e6

this code was originally used to process a large batch of records retrieved from
a database:
  - database records were fetched
  - a list of unique document IDs were compiled from the fetched records
  - this list of IDs was submitted to the ThreadPoolExecutor's target func,
    which called formatted a request URL and called an API to validate the document,
    then returned the ID and validation result
  - the response handler threading.Thread used the validation result to process
    and bucket the database records associated with the document ID into appropriately.
    in the original code the validated records were processed into requests for
    another API, which were made after this multithreaded validation process,
    due to rate limits on this second API

since all those APIs are behind firewalls, this has been simplified to use
https://httpbin.org endpoints

NOTE: uncomment out wait/sleep statements in the target funcs and decrease the
      number of workers for the first ThreadPoolExecutor to be able to read log
      statements and see the context switching happen.
'''
# updated: 2022-06-22

import os
import time
import json
import signal
import random
import logging
import logging.config
import requests
import threading
from collections import deque
import concurrent.futures as confute
from datetime import datetime


# CONFIGURE LOGGING
log_level = 'INFO'
log_filename = 'thrddds_dev.log'
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
logger = logging.getLogger('thrddds')
rlogger = logging.getLogger('data_requester')
hlogger = logging.getLogger('data_handler')
logger.info('- - - - - - - - - - - - - - - - -')
logger.info('logging configured')


def wait(mins=0.2, maxs=0.7):
    time.sleep(mins + random.random() * (maxs - mins))


def request_data(docid, url, rqueue):
    if stop_event.is_set():
        return
    try:
        rlogger.info(f'dispatching validation request for docid {docid}')
        r = requests.get(url)
        bundle = (docid, r)
    except Exception as e:
        # rlogger.exception(f'exception validating docid {docid}')
        bundle = (docid, e)
    rqueue.append(bundle)  # add response bundle to processing queue for response handler
    # wait()  # pause to space out API calls
    return bundle


def handle_response(rqueue, vqueue, notfounds):
    hlogger.info('ready to route responses')
    wait()  # wait for data to populate queue

    while not stop_event.is_set():
        # attempt to pull a response off the queue
        try:
            docid, r = rqueue.popleft()
            hlogger.info(f'popped response for {docid} off the queue')
        except IndexError:
            hlogger.info('nothing pending on the queue')
            wait()  # wait for the queue to populate
            continue
        else:
            hlogger.info(f'{len(rqueue)} items pending on the queue')

        # parse the response
        if not isinstance(r, requests.models.Response):  # response is actually an exception
            hlogger.error(f'exception thrown for docid {docid} validation request')
            notfounds.extend(
                [
                    {
                        'docid': docid,
                        'status_code': r.__class__.__name__,
                        'reason': r.__doc__,
                        'response_text': r.__str__()
                    }
                ]
            )
            wait()
            continue
        if r.status_code != 200:  # response does not indicate success
            hlogger.error(f'validation request failed for docid {docid}')
            notfounds.extend(
                [
                    {
                        'docid': docid,
                        'status_code': r.status_code,
                        'reason': r.reason,
                        'response_text': r.text
                    }
                ]
            )
        else:  # successful response, add records to the requests queue
            hlogger.info(f'document validation successful for docid {docid}')
            vqueue.extend([(docid, r.json()['uuid']) for _ in range(random.randint(1, 5))])
            if 32 < len(vqueue) % 50 < 48:
                hlogger.info(f'{len(vqueue)} records validated')
        time.sleep(0.05)
    hlogger.info('i have lunch at the same time everyday')


def output_json(jobj, filepath=None):
    with open(filepath, 'a') as outf:
        outf.write(json.dumps(jobj))


def handle_kb_interrupt(sig, frame):
    '''
    intercept KeyboardInterrupt and set threading.Event, which causes threads
    to exit and shutdown gracefully. stop_event is initialized in main thread.
    '''
    stop_event.set()


if __name__ == "__main__":
    # thread resources
    request_url = 'https://httpbin.org/uuid'
    stop_event = threading.Event()
    signal.signal(signal.SIGINT, handle_kb_interrupt)
    response_queue = deque()
    validated_queue = deque()
    notfounds = deque()
    num_db_records_synthetic = 234
    docids_synthetic = [random.randint(1234, 23432) for _ in range(num_db_records_synthetic)]
    stopwatch_start = datetime.now()

    # start up the response router first, since the call to threadpoolexceutor blocks the main thread
    router = threading.Thread(target=handle_response, name='response_handler', args=(response_queue, validated_queue, notfounds))
    router.start()

    while not stop_event.is_set():
        # launch the threadpoolexecutor using a context manager, which will wait (block)
        # until all pending futures are executed and their resources freed
        # https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.Executor.shutdown
        with confute.ThreadPoolExecutor(max_workers=8) as executor:
            logger.info(f'submitting {len(docids_synthetic)} docids for validation by target func "{request_data.__name__}" to the pool')
            futures = [executor.submit(request_data, docid, request_url, response_queue) for docid in docids_synthetic]
        logger.info('all document validation tasks are done!!')

        while response_queue:  # wait for the response queue to be processed by router
            if stop_event.is_set():
                break
            time.sleep(1)

        logger.info('all responses parsed by the router!!')
        if len(notfounds) > 0:
            logger.warning(f'unable to validate {len(notfounds)} documents out of {docids_synthetic} total docs')
            logger.warning(f'logging unvalidated docids with their response data')
            output_json(list(notfounds), filepath=os.path.join('logs', 'notfound.json'))
        else:
            logger.info('all documents validated!!')
        time.sleep(1)
        stop_event.set()

    stopwatch_stop = datetime.now()
    logger.info('shutting down...')
    time.sleep(1)
    router.join()
    logger.info(f'total time elapsed: {stopwatch_stop - stopwatch_start}')
