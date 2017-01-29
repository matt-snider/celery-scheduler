from datetime import timedelta

import pymongo
from bson import ObjectId
from celery.beat import Scheduler, ScheduleEntry
from celery.schedules import schedule, crontab
from celery.utils.log import get_logger


logger = get_logger('scheduler')


class MongoScheduleEntry(ScheduleEntry):

    def __init__(self, _id=None, enabled=True, **kwargs):
        super().__init__(**kwargs)
        self._id = _id or ObjectId()
        self.enabled = enabled


class MongoScheduler(Scheduler):

    Entry = MongoScheduleEntry

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        db_settings = self.app.conf.CELERYBEAT_MONGODB_BACKEND_SETTINGS
        db, col = db_settings['database'], db_settings['schedule_collection']

        self.client = pymongo.MongoClient(self.app.conf.CELERYBEAT_BACKEND)
        self.scheduler_collection = self.client[db][col]
        self.sync()

    def sync(self):
        new_schedule = {doc['name']: self.schedule_from_doc(doc)
                        for doc in self.scheduler_collection.find()
                        if doc.get('enabled', True)}

        # Save state of the current entries
        for name, entry in self.schedule.items():
            self.scheduler_collection.update(
                {'_id': entry._id},
                {'$set': {
                    'last_run_at': entry.last_run_at,
                    'total_run_count': entry.total_run_count
                    }},
            )

        # Replace schedule with new schedule
        new_schedule = {doc['name']: self.schedule_from_document(doc)
                        for doc in self.scheduler_collection.find()
                        if doc.get('enabled', True)}
        self.merge_inplace(new_schedule)

        # Trigger recreation of the heap
        self._heap = None

    def schedule_from_document(self, doc):
        if 'crontab' in doc:
            doc['schedule'] = crontab(**doc.pop('crontab'))
        elif 'interval' in doc:
            interval = doc.pop('interval')
            run_every = timedelta(**{interval['period']: interval['every']})
            doc['schedule'] = schedule(run_every)
        else:
            raise Exception('Bad schedule')
        return doc

    def get_schedule(self):
        return self.schedule

    def set_schedule(self, schedule):
        pass

    def close(self):
        self.sync()
        # Close connections

