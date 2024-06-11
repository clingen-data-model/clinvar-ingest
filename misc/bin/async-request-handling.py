#!/usr/bin/env python3
import logging
import multiprocessing
import time

import requests
from fastapi import BackgroundTasks, FastAPI, Request

formatter = logging.Formatter(
    fmt="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logging.getLogger().addHandler(handler)
logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger(__name__)


baseurl = "http://localhost:8000"


app = FastAPI(openapi_url="/openapi.json", docs_url="/api")


@app.get("/sleep_async_BackgroundTask_busy/{seconds}")
async def sleep_async_BackgroundTask_busy(
    request: Request, background_tasks: BackgroundTasks, seconds: int
):
    async def task():
        start = time.time()
        while time.time() - start < seconds:
            pass

    background_tasks.add_task(task)
    return {}


@app.get("/sleep_sync_BackgroundTask_busy/{seconds}")
async def sleep_sync_BackgroundTask_busy(
    request: Request, background_tasks: BackgroundTasks, seconds: int
):
    def task():
        start = time.time()
        while time.time() - start < seconds:
            pass

    background_tasks.add_task(task)
    return {}


@app.get("/async_do_nothing_and_return/{seconds}")
async def async_do_nothing_and_return(request: Request, seconds: int):
    return {}


def send_request(request_name: str = "sleep_1", seconds=5):
    logger.info(f"Sending request to {request_name} for {seconds} seconds")
    response = requests.get(f"{baseurl}/{request_name}/{seconds}", timeout=60)
    logger.info(f"{request_name} responded with {response.status_code}")
    logger.info(response.json())
    return response


def scenario1():
    """
    async handler, async BackgroundTask, async followup
    """
    logger.info("Starting scenario 1")
    p1 = multiprocessing.Process(
        target=send_request, args=("sleep_async_BackgroundTask_busy", 30)
    )
    p1.start()

    # need to definitively make sure p1 has started before p2. short sleep is enough
    time.sleep(3)

    p2 = multiprocessing.Process(
        target=send_request, args=("async_do_nothing_and_return", 0)
    )
    p2.start()

    ps = {"p1": p1, "p2": p2}
    while len(ps.keys()):
        del_keys = []
        for p_name, p in ps.items():
            try:
                p.join(timeout=0)
                if not p.is_alive():
                    logger.info(f"{p_name} joined")
                    del_keys.append(p_name)
            except TimeoutError:
                pass
        for del_key in del_keys:
            del ps[del_key]


def scenario2():
    """
    async handler, sync BackgroundTask, async followup
    """
    logger.info("Starting scenario 5")
    p1 = multiprocessing.Process(
        target=send_request, args=("sleep_sync_BackgroundTask_busy", 30)
    )
    p1.start()

    # need to definitively make sure p1 has started before p2. short sleep is enough
    time.sleep(3)

    p2 = multiprocessing.Process(
        target=send_request, args=("async_do_nothing_and_return", 0)
    )
    p2.start()

    ps = {"p1": p1, "p2": p2}
    while len(ps.keys()):
        del_keys = []
        for p_name, p in ps.items():
            try:
                p.join(timeout=0)
                if not p.is_alive():
                    logger.info(f"{p_name} joined")
                    del_keys.append(p_name)
            except TimeoutError:
                pass
        for del_key in del_keys:
            del ps[del_key]


if __name__ == "__main__":
    #
    # Run app with
    # uvicorn async-request-handling:app --reload
    #
    logger.info("Starting client requests")
    scenario2()


# pylint: disable=W0105
"""
# Scenario 1 Run Log

Shell 1:
```
$ uvicorn async-request-handling:app
INFO:     Started server process [94215]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
INFO:     127.0.0.1:60134 - "GET /sleep_async_BackgroundTask_busy/30 HTTP/1.1" 200 OK
INFO:     127.0.0.1:60139 - "GET /async_do_nothing_and_return/0 HTTP/1.1" 200 OK
```

Shell 2:
```
$ python async-request-handling.py
2024-01-29 22:28:20 INFO     Starting client requests
2024-01-29 22:28:20 INFO     Starting scenario 1
2024-01-29 22:28:21 INFO     Sending request to sleep_async_BackgroundTask_busy for 30 seconds
2024-01-29 22:28:21 INFO     sleep_async_BackgroundTask_busy responded with 200
2024-01-29 22:28:21 INFO     {}
2024-01-29 22:28:23 INFO     p1 joined
2024-01-29 22:28:24 INFO     Sending request to async_do_nothing_and_return for 0 seconds
2024-01-29 22:28:51 INFO     async_do_nothing_and_return responded with 200
2024-01-29 22:28:51 INFO     {}
2024-01-29 22:28:51 INFO     p2 joined
```

# Scenario 2 Run Log

Shell 1:
```
$ uvicorn async-request-handling:app
INFO:     Started server process [94890]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
INFO:     127.0.0.1:60251 - "GET /sleep_sync_BackgroundTask_busy/30 HTTP/1.1" 200 OK
INFO:     127.0.0.1:60253 - "GET /async_do_nothing_and_return/0 HTTP/1.1" 200 OK
```

Shell 2:
```
python async-request-handling.py
2024-01-29 22:37:07 INFO     Starting client requests
2024-01-29 22:37:07 INFO     Starting scenario 5
2024-01-29 22:37:07 INFO     Sending request to sleep_sync_BackgroundTask_busy for 30 seconds
2024-01-29 22:37:07 INFO     sleep_sync_BackgroundTask_busy responded with 200
2024-01-29 22:37:07 INFO     {}
2024-01-29 22:37:10 INFO     p1 joined
2024-01-29 22:37:10 INFO     Sending request to async_do_nothing_and_return for 0 seconds
2024-01-29 22:37:10 INFO     async_do_nothing_and_return responded with 200
2024-01-29 22:37:10 INFO     {}
2024-01-29 22:37:10 INFO     p2 joined
```

# Conclusion

Sending an async task to BackgroundTasks seems to be categorically worse than sending a sync task to BackgroundTasks. The async task will block the main thread, while the sync task will not. This is true even if the async task is followed by an async task, as in scenario 1. The async task will block the main thread. Even if the next request is async, it will not be processed until the first async task is finished. This is not the case with sync tasks. The sync task will not block the main thread, and the next request will be processed immediately. This is true even if the next request is async, as in scenario 2.
"""
