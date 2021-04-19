import threading
import schedule
import time

# 1. import tasks here
from tasks import author, media, tags, wordcount, timeline

def run_threaded(task):
    job_thread = threading.Thread(target=task.run)
    job_thread.start()

# 2. schedule tasks here
schedule.every(30).seconds.do(run_threaded, author)
schedule.every(30).seconds.do(run_threaded, media)
# schedule.every(3).seconds.do(run_threaded, sources)
schedule.every(30).seconds.do(run_threaded, tags)
schedule.every(30).seconds.do(run_threaded, wordcount)
schedule.every(30).seconds.do(run_threaded, timeline)

while 1:
    schedule.run_pending()
    time.sleep(1)