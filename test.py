# 会在数据库中生成scheduler表，如果每次启动都执行add_job函数，则会在数据库中存放多个相同的作业，比如my_job1会存在多条记录，就会被反复调用，相当于并行执行了。
import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
# an SQLAlchemyJobStore named “default” (using SQLite)
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
# a ThreadPoolExecutor named “default”, with a worker count of 20
# a ProcessPoolExecutor named “processpool”, with a worker count of 5
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from dbjobstore import DbJobStore

# 使用mysql存储作业
# url = 'mysql://hr:123456@localhost/stock'
url='postgresql+psycopg2://postgres:dbadmin@127.0.0.1:5432/scheduler'

# 其他的存储方式可以参考：
# http://apscheduler.readthedocs.io/en/latest/modules/jobstores/sqlalchemy.html#apscheduler.jobstores.sqlalchemy.SQLAlchemyJobStore
# http://docs.sqlalchemy.org/en/latest/core/engines.html?highlight=create_engine#database-urls

def my_job1():
    print(datetime.datetime.now())
    print('-'*10+'my_job1'+'-'*10)

def my_job2(text):
    print(datetime.datetime.now())
    print('-' * 10 + text + '-' * 10)

def my_job3():
    print(datetime.datetime.now())
    print('-' * 10 + 'my_job3' + '-' * 10)

jobstores = {
    'default': DbJobStore(url=url)
#    'default': SQLAlchemyJobStore(url=url)
}
executors = {
    'default': ThreadPoolExecutor(20),
    'processpool': ProcessPoolExecutor(5)
}
job_defaults = {
    'coalesce': False,
    'max_instances': 3
}

sched = BlockingScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults)
sched.add_job(my_job1, trigger='interval', seconds=3, id='my_job1')
sched.add_job(my_job2, trigger='cron', minute='*', args=['my_job2'], id='my_job2')
sched.add_job(my_job3, trigger='date', run_date=datetime.datetime(2017, 10, 14, 13, 4, 5), id='my_job3')
sched.start()
