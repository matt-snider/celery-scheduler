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
        self.client = pymongo.MongoClient(self.app.conf.CELERYBEAT_BACKEND)
        self.mongo_settings = self.app.conf.CELERYBEAT_MONGODB_BACKEND_SETTINGS

        db_name = self.mongo_settings['database']
        collection = self.mongo_settings['schedule_collection']
        self.scheduler_collection = self.client[db_name][collection]
        self.sync()

    def sync(self):
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

    def close(self):
        self.sync()
        # Close connections

    @property
    def info(self):
        return ('    . db -> MongoDB(database={database}, '
                'collection={schedule_collection})'
                    .format(**self.mongo_settings))

