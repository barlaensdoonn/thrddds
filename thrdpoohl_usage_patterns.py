'''
from: https://superfastpython.com/threadpoolexecutor-in-python/
python docs: https://docs.python.org/3/library/concurrent.futures.html#
other references:
  - https://stackoverflow.com/a/58829816
  - https://realpython.com/python-deque/#sharing-data-between-threads
  - https://alexandra-zaharia.github.io/posts/how-to-stop-a-python-thread-cleanly/#by-default-the-thread-is-not-stopped-cleanly
'''
# updated: 2022-06-18

import os
import logging
import logging.config
from urllib.request import urlopen
import concurrent.futures as cute


# CONFIGURE LOGGING
log_level = 'INFO'
log_filename = 'thrdddspoohl.log'
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
logger = logging.getLogger('thrdpoohl_play')
logger.info('- - - - - - - - - - - - - - - - -')
logger.info('logging configured')


writepath = os.path.join('output', 'urls')  # filepath for writing files

# python concurrency API docs
urls = ['https://docs.python.org/3/library/concurrency.html',
        'https://docs.python.org/3/library/concurrent.html',
        'https://docs.python.org/3/library/concurrent.futures.html',
        'https://docs.python.org/3/library/threading.html',
        'https://docs.python.org/3/library/multiprocessing.html',
        'https://docs.python.org/3/library/multiprocessing.shared_memory.html',
        'https://docs.python.org/3/library/subprocess.html',
        'https://docs.python.org/3/library/queue.html',
        'https://docs.python.org/3/library/sched.html',
        'https://docs.python.org/3/library/contextvars.html']


def _example_with_yield_in_main_thread():
    '''
    example of how to call the parent function and access the results.
    without the log statement, this could be a list comprehension:

    futes = fute for fute in submit_wait_as_completed(urls)
    '''
    futes = []
    for fute in submit_wait_as_completed(urls):
        logger.info('found a fute')
        futes.append(fute)


def _download_and_return(url):
    try:
        logger.info(f'opening connection to {url}')
        with urlopen(url, timeout=3) as connection:
            return (connection.read(), url)
    except Exception:
        logger.exception(f'exception thrown when trying to access {url}')
        return (None, url)


def _download_and_write(url):
    '''download data from URL and write it out to disk'''
    writepath = os.path.join('output', 'urls')  # filepath for writing files
    with urlopen(url, timeout=3) as connection:
        data = connection.read()
    if data is None:
        logger.error(f'error downloading {url}')
        return (url, None)
    else:
        outpath = os.path.join(writepath, os.path.basename(url))
        with open(outpath, 'wb') as file:
            file.write(data)
        logger.info(f'wrote out {os.path.basename(url)} to {outpath}')
        return (url, outpath)


def submit_no_wait(urls, writepath):
    '''
    submit individual threads for each task via a list comprehension.
    this executes the tasks concurrently, and returns the the futures immediately.
    futures are discarded with assignment to '_' throwaway variable.
    '''
    with cute.ThreadPoolExecutor(max_workers=len(urls)) as executor:
        _ = [executor.submit(_download_and_write, url, writepath) for url in urls]


def submit_wait_all_completed(urls, writepath, timeout=None, condition=cute.ALL_COMPLETED):
    '''
    submit individual threads for each task via a list comprehension.
    this executes the tasks concurrently, and returns the the futures immediately.
    here the list of futures returned by each submit() are captured, and then waited
    upon. however the results of the call to wait() are discarded... this means
    the wait is only useful if blocking the calling thread until the condition
    specified by wait()'s '@return__when kwarg is satisfied.

    @timeout:      represents wait()'s optional @timeout kwarg. accepts int, float, or None.
                   if not specified, or set to None, then no timeout is implemented,
                   and there is no limit to how long the function will wait.

                   if set, it specifies the maximum number of seconds to wait before
                   returning. if the timeout is reached and wait is still executing,
                   it will return regardless of the state of the futures being waited
                   upon. it returns a named 2-tuple of sets:
                     - the first - called 'done' - contains futures that completed
                       (state == finished or cancelled) before the timeout.
                     - the second - called 'not_done' - holds the uncompleted
                       futures (state == pending or running)

    @return_when:  represents wait()'s @return_when kwarg. specifies the condition
                   that when met, causes wait() to return. accepts three possible
                   values, which are concurrent.futures constants:
                     - ALL_COMPLETED:    return when all futures are completed or cancelled.
                     - FIRST_COMPLETED:  return when any future completes or is cancelled.
                     - FIRST_EXCEPTION:  return when any future finishes by raising an exception.
                                         if no future raises an Exception, it is equivalent to ALL_COMPLETED.
                   defaults to concurrent.futures.ALL_COMPLETED. the other two
                   available constants are FIRST_COMPLETED and FIRST_EXCEPTION.
    '''
    with cute.ThreadPoolExecutor(max_workers=len(urls)) as executor:
        futures = [executor.submit(_download_and_write, url, writepath) for url in urls]
        _, _ = cute.wait(futures, timeout=timeout, return_when=condition)


def submit_wait_as_completed(urls):
    '''
    submit tasks (download a list of urls) to a ThreadPoolExecutor to be executed
    concurrently. iterate over concurrent.futures.as_completed() to get results
    from the futures as they complete, one at a time, with no expectation they
    will complete/return in the order they were submitted. act on the results serially,
    in the same for loop iteration that receives the completed (or cancelled) future's results.

    the yield is optional; easier to loop over this as a generator than copy/paste
    the body of the entire function. idk if it's best practice, or if it matters or not.
    '''
    with cute.ThreadPoolExecutor(max_workers=len(urls)) as executor:
        logger.info(f'submitting {len(urls)} tasks to ThreadPoolExecutor')
        futures = [executor.submit(_download_and_return, url) for url in urls]
        logger.info(f'received {len(futures)} future objects after submission to the pool')

        for future in cute.as_completed(futures):
            data, url = future.result()
            if data is None:
                logger.error(f'error: no data returned for url {url}')
                continue
            # do something with the results here, like save the data to disk
            logger.info(f'succesfully fetched data of length {len(data)} for url {url}')
            logger.info(f'data is of type {type(data)}')
            yield future


def map_w_result(urls):
    '''
    ThreadPoolExecutor.map() submits all the tasks to the pool simultaneously...
    in this way it is similar to the submit() usage patterns; however when iterated
    over, it returns a result (if any) instead of a future object.

    NOTE: difference between executor.map() and the python builtin map(), is the
    builtin evaluates each call one-by-one during iteration ("lazy-evaluation"),
    whereas executor.map() launches them to run concurrently. this means
    that executor.map() will issue and execute all provided tasks regardless of
    whether the results are iterated over or not. see the final example here:
    https://superfastpython.com/threadpoolexecutor-in-python/#Map_and_Wait_Pattern

    executor.map() differs from the submit()/wait() usage patterns in that it
    returns the results sequentially in the order the tasks were sent to the pool.
    '''
    with cute.ThreadPoolExecutor() as executor:
        for result in executor.map(_download_and_write, urls):
            if None in result:
                logger.error(f'error occurred when executing target function for {result[0]}')
            else:
                logger.info(f'{result[0]} successfully retrieved and written to {result[1]}')
            yield result


if __name__ == '__main__':
    for r in map_w_result(urls):
        logger.info(f'received from map_w_result(): {r}')
