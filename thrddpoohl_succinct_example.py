# from: https://gist.github.com/magnetikonline/a26ae80e2e23fcfda5b03ccb470f79e6

import threading
import time
import random
from collections import deque
from concurrent import futures

WORKER_COUNT = 4


def worker_init():
    work = deque()
    result = deque()
    finished = threading.Event()
    pool = futures.ThreadPoolExecutor(WORKER_COUNT)

    # add workers into pool
    for _ in range(WORKER_COUNT):
        pool.submit(worker, work, result, finished)

    return (pool, work, result, finished)


def worker(work, result, finished):
    while True:
        task = None
        try:
            task = work.pop()
        except IndexError:
            pass

        if task is None:
            if finished.is_set():
                # no more tasks
                break

            # pause, then re-check work queue
            time.sleep(1)
            continue

        # dummy processing of task and store result
        print(f"processing task: {task}")
        time.sleep(random.randrange(1, 2))

        result.append(random.randrange(1, 50))

    # worker finished


def main():
    # setup workers in a thread pool and work/result queues
    pool, work, result, finished = worker_init()

    # populate queue with example "work"
    for task_number in range(50):
        work.appendleft(task_number)

    # mark queue as finished/no more "work"
    finished.set()

    # await for workers to finish up
    pool.shutdown()
    print(result)


if __name__ == "__main__":
    main()
