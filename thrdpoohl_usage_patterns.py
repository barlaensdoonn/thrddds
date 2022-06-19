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
import time
import logging
import logging.config
from datetime import datetime
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


def download_and_return(url):
    '''download data from URL and return it'''
    try:
        logger.info(f'opening connection to {url}')
        with urlopen(url, timeout=3) as connection:
            bundle = (connection.read(), url)
            logger.info(f'successfully read data from {url}')
            return bundle
    except Exception:
        logger.exception(f'exception thrown when trying to access {url}')
        return (None, url)


def download_and_write(url, writepath=writepath):
    '''download data from URL and write it to disk'''
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


def nap_and_dream(nappp, dreamabout):
    dreams = os.path.basename(dreamabout)
    napper = logging.getLogger(dreams)
    napper.info(f"oh it's {datetime.now().time()}, time for a nap")
    time.sleep(nappp)
    napper.info(f'i was out for {nappp} seconds, dreaming about {dreams}')
    napper.info(f"now it's {datetime.now().time()}")
    return nappp, dreams


def submit_no_wait(func=download_and_write, params=urls):
    '''
    submit individual threads for each task via a list comprehension.
    this executes the tasks concurrently, and returns the the futures immediately.
    futures are discarded with assignment to '_' throwaway variable.
    '''
    with cute.ThreadPoolExecutor(max_workers=len(params)) as executor:
        _ = [executor.submit(func, param) for param in params]


def submit_wait_all_completed(func=download_and_write, params=urls, timeout=None, condition=cute.ALL_COMPLETED):
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
    with cute.ThreadPoolExecutor(max_workers=len(params)) as executor:
        futures = [executor.submit(func, param) for param in params]
        _, _ = cute.wait(futures, timeout=timeout, return_when=condition)


def submit_wait_as_completed(func=download_and_return, params=urls):
    '''
    submit tasks (download a list of urls) to a ThreadPoolExecutor to be executed
    concurrently. iterate over concurrent.futures.as_completed() to get results
    from the futures as they complete, one at a time, with no expectation they
    will complete/return in the order they were submitted. act on the results serially,
    in the same for loop iteration that receives the completed (or cancelled) future's results.

    the yield is optional; easier to loop over this as a generator than copy/paste
    the body of the entire function. idk if it's best practice, or if it matters or not.
    '''
    with cute.ThreadPoolExecutor(max_workers=len(params)) as executor:
        logger.info(f'submitting {len(params)} tasks to pool')
        futures = [executor.submit(func, param) for param in params]

        # process results... this would change depending on the pool's target func + params
        for future in cute.as_completed(futures):
            data, url = future.result()
            if data is None:
                logger.error(f'error: no data returned for url {url}')
                continue
            # do something with the results here, like save the data to disk
            logger.info(f'succesfully fetched data of length {len(data)} for url {url}')
            logger.info(f'data is of type {type(data)}')
            yield future


def submit_get_sequentially(func=download_and_return, params=urls):
    '''
    instead of using as_completed() to grab results from whatever thread is done
    first, retrieve results sequentially, in the order that the threads were
    submitted to the pool.

    similar to the outcome of ThreadPoolExecutor.map()
    '''
    with cute.ThreadPoolExecutor() as executor:
        futures = [executor.submit(func, params) for param in params]

    # process results sequentially, as the futures iterable is ordered
    # based on the call to submit()
    for future in futures:
        logger.info(f'future state: {future.state()}')


def map_w_result(func=download_and_write, params=urls):
    '''
    ThreadPoolExecutor.map() submits all the tasks to the pool simultaneously...
    in this way it is similar to the submit() usage patterns; however when iterated
    over, it returns a result (if any) instead of a future object.

    NOTE: executor.map() executes the parameterized calls to the target function
    asyncronously and concurrently (if enough threads/resources are available);
    as a consquence, all the specified tasks will be executed regardless of whether
    the iterator returned by executor.map() is iterated over or not. this is different
    than python's builtin map(), as the builtin evaluates each task one-by-one during
    iteration ("lazy-evaluation").
    see the examples in this section for reference:
    https://superfastpython.com/threadpoolexecutor-in-python/#Map_and_Wait_Pattern

    executor.map() differs from most submit()/wait() usage patterns in that it
    returns results sequentially in the order of the parameters in the iterator(s)
    that populate the call to the target function.

    @timeout: int, float, or None. specifies the length of the timeout in seconds.
              during iteration over the call to executor.map(), if a result isn't available
              after the timeout, a concurrent.futures.TimeoutError is raised. the
              countdown starts with the original call to executor.map(). if not
              specified or None, there is no limit to how long to wait for results.
    '''
    with cute.ThreadPoolExecutor() as executor:
        for result in executor.map(func, params, timeout=None):
            if None in result:
                logger.error(f'error occurred when executing target function for {result[0]}')
            else:
                logger.info(f'{result[0]} successfully retrieved and written to {result[1]}')
            yield result


def submit_w_callbacks(func=nap_and_dream, params=urls):
    '''
    add callbacks onto the tasks after they are submitted. possible to add more than
    one callback; they'll be called in the order they're registered. the callbacks
    are executed on a future when it completes, original order is irrelevant
    '''
    callog = logging.getLogger('callog')

    def callback(fute):
        callog.info(f'callback: {fute.result()[0]}')

    def callbackk(fute):
        callog.info(f'callback: {fute.result()[1]}')

    # make params for target func with enumerate, but reverse the count
    # so the first thread submitted sleeps the longest
    naps = [i+1 for i in range(len(params))]
    naps.reverse()
    napdreams = [(naptime, url) for naptime, url in zip(naps, params)]

    with cute.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(func, *napdream) for napdream in napdreams]
        # register the callbacks on all tasks
        for future in futures:
            future.add_done_callback(callback)
            future.add_done_callback(callbackk)


if __name__ == '__main__':
    submit_w_callbacks()
