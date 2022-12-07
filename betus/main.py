import schedule
import time
import os

# Si quieres los datos inmediatos
os.system('scrapy crawl betusraces')

print('Scheduler initialised')
schedule.every(5).minutes.do(lambda: os.system('scrapy crawl betusraces'))
print('Next job is set to run at: ' + str(schedule.next_run()))

while True:
    schedule.run_pending()
    time.sleep(1)
    
