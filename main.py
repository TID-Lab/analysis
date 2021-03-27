import threading
import schedule
import time

# 1. import tasks here
from tasks import author, media

def run_threaded(task):
    job_thread = threading.Thread(target=task.run)
    job_thread.start()

# 2. schedule tasks here
schedule.every(3).seconds.do(run_threaded, author)
schedule.every(5).seconds.do(run_threaded, media)

while 1:
    schedule.run_pending()
    time.sleep(1)