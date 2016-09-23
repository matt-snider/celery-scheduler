from bson import ObjectId

from celery.beat import Scheduler, ScheduleEntry


class MongoScheduleEntry(ScheduleEntry):

    def __init__(self, _id=None, **kwargs):
        super().__init__(**kwargs)
        self._id = _id or ObjectId()


class MongoScheduler(Scheduler):

    Entry = MongoScheduleEntry

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # TODO: database setup
        self.scheduler_collection = None

    def sync(self):
        new_schedule = {doc['name']: doc
                        for doc in self.scheduler_collection.find()
                        if doc['enabled']}
        for name, entry in self.schedule.items():
            if name in new_schedule:
                self.scheduler_collection.update(
                    {'_id': entry._id},
                    {'last_run_at': entry.last_run_at,
                     'total_run_count': entry.total_run_count},
                )
            else:
                doc = vars(entry)
                doc.pop('app')
                self.scheduler_collection.insert_one(doc)

        self.merge_inplace(new_schedule)

    def get_schedule(self):
        return self.schedule

    def set_schedule(self, schedule):
        pass

    def close(self):
        self.sync()
        # Close connections

