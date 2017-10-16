from __future__ import absolute_import

import json
import datetime
from datetime import datetime
from copy import deepcopy
from apscheduler.jobstores.base import BaseJobStore, JobLookupError, ConflictingIdError
from apscheduler.util import ref_to_obj, maybe_ref, datetime_to_utc_timestamp, utc_timestamp_to_datetime, astimezone, convert_to_datetime
from apscheduler.job import Job
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.expression import null
from sqlalchemy import create_engine, Table, Column, MetaData, Unicode, Float, Integer, SmallInteger, String, Text, DateTime, LargeBinary, select, TIMESTAMP
from tzlocal import get_localzone

cronMap = {'second':'S', 'minute':'M', 'hour':'H', 'day':'d', 'month':'m', 'day_of_week':'w', 'week':'W', 'year':'Y'}
timezone = get_localzone()
class DbJobStore(BaseJobStore):
    """
    Stores jobs in a database table using SQLAlchemy.
    The table will be created if it doesn't exist in the database.

    Plugin alias: ``sqlalchemy``

    :param str url: connection string (see `SQLAlchemy documentation
        <http://docs.sqlalchemy.org/en/latest/core/engines.html?highlight=create_engine#database-urls>`_
        on this)
    :param engine: an SQLAlchemy Engine to use instead of creating a new one based on ``url``
    :param str tablename: name of the table to store jobs in
    :param metadata: a :class:`~sqlalchemy.MetaData` instance to use instead of creating a new one
        highest available
    """

    def __init__(self, url=None, engine=None, tablename='scheduler', metadata=None):
        super(DbJobStore, self).__init__()
        metadata = maybe_ref(metadata) or MetaData()

        if engine:
            self.engine = maybe_ref(engine)
        elif url:
            self.engine = create_engine(url)
        else:
            raise ValueError('Need either "engine" or "url" defined')

        # 191 = max key length in MySQL for InnoDB/utf8mb4 tables,
        # 25 = precision that translates to an 8-byte float
        self.jobs_t = Table(
            tablename, metadata,
            Column('id', Unicode(191, _warn_on_bytestring=False), primary_key=True),
            Column('name', String(100), index=False, default=''),            #my_job2
            Column('func', String(500), index=False, default=''),            #__main__:my_job2
            Column('args', String(500), index=False, default=''),            #('my_job2')
            Column('kwargs', String(500), index=False, default=''),          #{}
            Column('version', String(10), index=False, default=''),          #版本，默认是1
            Column('trigger_type', Text, index=False),                       #触发器类型   interval/cron/date 
            Column('crontab', String(1000), index=False),                    #crontab
            Column('interval', Integer, index=False),                        #interval
            Column('run_date', TIMESTAMP(timezone=True), index=False),       #datetime
            Column('coalesce', Integer, index=False),                        #False/True, default:False
            Column('start_date', TIMESTAMP(timezone=True), index=False),     #开始时间，针对crontab/interval
            Column('end_date', TIMESTAMP(timezone=True), index=False),       #结束时间，针对crontab/interval
            Column('next_run_time', TIMESTAMP(timezone=True), index=False),  #下次执行时间
            Column('max_instances', Integer, index=False, default=''),       #3
            Column('executor', String(50), index=False, default=''),         #default
            Column('misfire_grace_time', Integer, index=False, default=''),  #过时任务，是否补齐
        )

    def start(self, scheduler, alias):
        super(DbJobStore, self).start(scheduler, alias)
        self.jobs_t.create(self.engine, True)

    def lookup_job(self, job_id):
        selectable = select([self.jobs_t.c.id, self.jobs_t.c.name,self.jobs_t.c.func,self.jobs_t.c.args, self.jobs_t.c.kwargs, 
            self.jobs_t.c.version, self.jobs_t.c.trigger_type, self.jobs_t.c.crontab, self.jobs_t.c.interval, self.jobs_t.c.run_date,
            self.jobs_t.c.coalesce, self.jobs_t.c.next_run_time, self.jobs_t.c.executor, self.jobs_t.c.misfire_grace_time, 
            self.jobs_t.c.max_instances, self.jobs_t.c.start_date, self.jobs_t.c.end_date]).where(self.jobs_t.c.id == job_id)
        row = self.engine.execute(selectable).scalar()
        return self._db_to_job(row) if row else None

    def get_due_jobs(self, now):
        timestamp = datetime_to_utc_timestamp(now)
        return self._get_jobs(self.jobs_t.c.next_run_time <= datetime.fromtimestamp(timestamp))

    def get_next_run_time(self):
        selectable = select([self.jobs_t.c.next_run_time]).where(self.jobs_t.c.next_run_time != null())
        selectable = selectable.order_by(self.jobs_t.c.next_run_time).limit(1)
        next_run_time = self.engine.execute(selectable).scalar()
        return utc_timestamp_to_datetime(next_run_time.timestamp()) if next_run_time else ''

    def get_all_jobs(self):
        jobs = self._get_jobs()
        self._fix_paused_jobs_sorting(jobs)
        return jobs

    def add_job(self, job):
        insertData = self._job_to_db(job)
        insert = self.jobs_t.insert().values(**insertData)
        try:
            self.engine.execute(insert)
        except IntegrityError:
            raise ConflictingIdError(job.id)

    def update_job(self, job):
        updateData = self._job_to_db(job)
        update = self.jobs_t.update().values(**updateData).where(self.jobs_t.c.id == job.id)
        result = self.engine.execute(update)
        if result.rowcount == 0:
            raise JobLookupError(id)

    def remove_job(self, job_id):
        delete = self.jobs_t.delete().where(self.jobs_t.c.id == job_id)
        result = self.engine.execute(delete)
        if result.rowcount == 0:
            raise JobLookupError(job_id)

    def remove_all_jobs(self):
        delete = self.jobs_t.delete()
        self.engine.execute(delete)

    def shutdown(self):
        self.engine.dispose()

    def _get_jobs(self, *conditions):
        jobs = []
        selectable = select([self.jobs_t.c.id, self.jobs_t.c.name, self.jobs_t.c.func,self.jobs_t.c.args, self.jobs_t.c.kwargs, 
            self.jobs_t.c.version, self.jobs_t.c.trigger_type, self.jobs_t.c.crontab, self.jobs_t.c.interval, self.jobs_t.c.run_date, 
            self.jobs_t.c.coalesce, self.jobs_t.c.next_run_time, self.jobs_t.c.executor, self.jobs_t.c.misfire_grace_time, 
            self.jobs_t.c.max_instances, self.jobs_t.c.start_date, self.jobs_t.c.end_date]).order_by(self.jobs_t.c.next_run_time)
        selectable = selectable.where(*conditions) if conditions else selectable
        failed_job_ids = set()
        for row in self.engine.execute(selectable):
            try:
                jobs.append(self._db_to_job(row))
            except:
                self._logger.exception('Unable to restore job "%s" -- removing it', row.id)
                failed_job_ids.add(row.id)

        # Remove all the jobs we failed to restore
        if failed_job_ids:
            delete = self.jobs_t.delete().where(self.jobs_t.c.id.in_(failed_job_ids))
            self.engine.execute(delete)

        return jobs

    def __repr__(self):
        return '<%s (url=%s)>' % (self.__class__.__name__, self.engine.url)

    def _db_to_job(self, row):
        if row['trigger_type'] == 'date': trigger = DateTrigger(run_date = row['run_date'])
        if row['trigger_type'] == 'cron':
            keys = row['crontab'].split(',')[1]
            values = row['crontab'].split(',')[0].split(' ')
            cronMapRev = {v:k for k,v in cronMap.items()}
            crontab = {cronMapRev[k]:values[i] for i,k in enumerate(keys)}
            trigger = CronTrigger(**crontab)
        if row['trigger_type'] == 'interval': trigger = IntervalTrigger(seconds=row['interval'])
        job = Job.__new__(Job)
        job.__setstate__({
            'id': row['id'],
            'name': row['name'],
            'func': row['func'],
            'args': json.loads(row['args']) if row['args'] else [],
            'kwargs': json.loads(row['kwargs']) if row['kwargs'] else {},
            'version': 1,
            'trigger': trigger,
            'executor': row['executor'],
            'start_date': row['start_date'],
            'end_date': row['end_date'],
            'next_run_time': utc_timestamp_to_datetime(row['next_run_time'].timestamp()),
            'coalesce': row['coalesce'],
            'misfire_grace_time': row['misfire_grace_time'],
            'max_instances': row['max_instances'],
            'jobstore': self,
        })
        job._scheduler = self._scheduler
        job._jobstore_alias = self._alias
        print(job._scheduler)
        print(job._jobstore_alias)
        return job

    def _job_to_db(self, job):
        row = {
            'id': job.id,
            'name': job.name,
            'func': "%s:%s" % (getattr(job.func, '__module__'), getattr(job.func, '__name__')), 
            'args': json.dumps(job.args),
            'kwargs': json.dumps(job.kwargs),
            'version': 1,
            'coalesce': int(job.coalesce),
            'trigger_type': '',
            'crontab': '',
            'interval': 0,
            'run_date': None,
            'start_date': None,
            'end_date': None,
            'next_run_time': job.next_run_time,
            'max_instances': job.max_instances, 
            'executor': job.executor, 
            'misfire_grace_time': job.misfire_grace_time, 
        }
        if isinstance(job.trigger, DateTrigger):
            row['trigger_type'] = 'date'
            row['run_date'] = job.trigger.run_date

        if isinstance(job.trigger, CronTrigger):
            cronSort = ['second', 'minute', 'hour', 'day', 'month', 'day_of_week', 'week', 'year']
            dt = {i.name:str(i) for i in job.trigger.fields}
            crontab = {'keys':[], 'values':[]}
            for k in cronSort:
                crontab['keys'].append(cronMap[k])
                crontab['values'].append(dt[k])

            row['trigger_type'] = 'cron'
            row['crontab'] =  " ".join(list(crontab['values']))+','+"".join(list(crontab['keys']))
            row['start_date'] =  job.trigger.start_date
            row['end_date'] =  job.trigger.start_date

        if isinstance(job.trigger, IntervalTrigger):
            row['trigger_type'] = 'interval'
            row['interval'] = job.trigger.interval.seconds
            row['start_date'] =  job.trigger.start_date
            row['end_date'] =  job.trigger.start_date
        return row
