# celery-scheduler
Database scheduler for celery supporting various backends.

## MongoScheduler
The scheduler requires the following celery settings to be set:

* **CELERYBEAT_BACKEND**: the url to the mongo instance (e.g. mongodb://localhost:27017/)
* **CELERYBEAT_MONGODB_BACKEND_SETTINGS**: a dictionary with the following keys
    * *database*: the mongo database to use (defaults to *celery*)
    * *schedule_collection*: the collection to store the schedules in (defaults to *periodic_tasks*)


To start the schedule use the *beat* command with the --scheduler property:
```shell
celery beat --scheduler celery_scheduler.scheduler.MongoScheduler
```

The scheduler expects documents in the following format:
```json
{
    "name" : "Testing testing",
    "task" : "tasks.add",
    "args" : [ 16, 16 ],
    "kwargs": {},
    "enabled" : true,
    "crontab|interval": {...},

    // Crontab
    // Any omitted fields will be interpreted as *
    // http://docs.celeryproject.org/en/latest/userguide/periodic-tasks.html#crontab-schedules
    "crontab" : {
        "minute": "*",
        "hour": "*",
        "day_of_week": "*",
        "day_of_month": "*",
        "month_of_year": "*"
    },

    // Interval
    // Matches the timedelta interface, allowing for a task to run
    // every N days, seconds, minutes, hours, or weeks
    // https://docs.python.org/3.6/library/datetime.html#timedelta-objects
    "interval": {
        "period": "days",   // or seconds, minutes, hours, weeks
        "every: 2
    }
}
```
