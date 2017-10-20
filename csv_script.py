#!/usr/bin/env python

import peewee
from peewee import *

from datetime import datetime, time, date, timedelta

# Local Server
db = MySQLDatabase("tembo",
                    host="localhost",
                    user="root",
                    passwd="1111")

# models
class america_account(peewee.Model):
    first_name = peewee.CharField()
    last_name = peewee.CharField()
    phone_number = peewee.CharField()
    email_address = peewee.CharField()
    stripe_customer_id = peewee.CharField()
    auth_type = peewee.IntegerField()
    active = peewee.BooleanField()
    profile_id = peewee.IntegerField()
    profile_type_id = peewee.IntegerField()

    class Meta:
        database = db


class america_parent(peewee.Model):
    LANGUAGES = (
        (0, 'English'),
        (1, 'Spanish'),
        (2, 'Arabic')
    )

    account_id = peewee.IntegerField()
    primary_language = peewee.IntegerField(choices=LANGUAGES, default=0)
    sms_preference_time = peewee.TimeField(default=time(12, 0, 0))  # 8am UTC
    sms_preference_timezone = peewee.CharField(max_length=200,
                                               default='Asia/Kuwait')

    class Meta:
        database = db


class america_subscription(peewee.Model):
    LEVELS = (
        (0, 'Basic'),
        (1, 'Deluxe'),
        (2, 'Premium'),
    )

    DURATIONS = (
        (0, 'Monthly'),
        (1, 'Trimester'),
        (2, 'Yearly'),
        (3, 'Lifetime'),
    )

    account_id = peewee.IntegerField()
    subscription_level = peewee.IntegerField(choices=LEVELS, default=0)
    subscription_duration = peewee.IntegerField(choices=DURATIONS,
                                                default=3,
                                                null=True)
    stripe_sub_plan_id = peewee.CharField(max_length=200,
                                          default='',
                                          null=True)
    active = peewee.BooleanField(default=True)

    class Meta:
        database = db


class america_schedule(peewee.Model):
    DAYS = (
        (0, 'Sunday'),
        (1, 'Monday'),
        (2, 'Tuesday'),
        (3, 'Wednesday'),
        (4, 'Thursday'),
        (5, 'Friday'),
        (6, 'Saturday'),
    )
    parent_id = peewee.IntegerField()
    day = peewee.IntegerField(choices=DAYS)

    class Meta:
        database = db


class america_child(peewee.Model):
    parent_id = peewee.IntegerField()
    first_name = peewee.CharField(max_length=200)
    last_name = peewee.CharField(max_length=200)
    birthday = peewee.DateField()
    active = peewee.BooleanField(default=True)
    welcome_message_sent = peewee.BooleanField(default=False)

    class Meta:
        database = db


class america_childprogress(peewee.Model):
    EVENT_STATES = (
        (0, 'Need Approval'),
        (1, 'Activity Ready'),
        (2, 'Activity Sent'),
        (3, 'Quiz Sent'),
        (4, 'Quiz Answered'),
        (5, 'Quiz Expired'),
        (21, 'Help'),
    )

    child_id = peewee.IntegerField()
    week = peewee.IntegerField(default=1)
    day = peewee.IntegerField(default=1)
    age = peewee.IntegerField()
    state = peewee.IntegerField(choices=EVENT_STATES, default=0)
    timestamp = peewee.DateTimeField()

    class Meta:
        database = db


# takes a string of days and returns int list representing those days
def parse_days(string_days):
    indices = {'Su': 0, 'M': 1, 'T': 2, 'W': 3, 'Th': 4, 'F': 5, 'Sa': 6}
    days = string_days.split("_")
    int_days = [1, 2, 3, 4, 5]
    max_days = 5
    if 1 <= len(days) <= max_days:
        try:
            int_days = list(map(lambda x: indices[x], days))
        except KeyError:
            pass
    return int_days

# script
for line in open("Test in Jordan.csv"):
    cleanList = line.strip().split(",")

    language = 0
    if cleanList[5] == 'Spanish':
        language = 1
    elif cleanList[5] == 'Arabic':
        language = 2

    users = america_account.select().where(
        (america_account.phone_number == cleanList[6]) |
        (america_account.email_address == cleanList[7])
    )

    # updates existing users with new information
    if users.exists():
        print("Duplicate user {number} {email}".format(number=cleanList[6],
                                                       email=cleanList[7]))

        user = users.first()

        # update language and sms preference time
        parent = america_parent.select().where(america_parent.account_id == user.id).get()
        parent.primary_language = language
        parent.sms_preference_time = (datetime.strptime(cleanList[8], '%H%M') - timedelta(hours=3)).time()
        parent.sms_preference_timezone = "Asia/Kuwait"
        parent.save()

        # remove current schedule
        schedule_days = america_schedule.select().where(america_schedule.parent_id == parent.id)
        for day in schedule_days:
            day.delete_instance()

        # add new schedule
        days = parse_days(cleanList[9])
        for i in range(len(days)):
            new_schedule = america_schedule(
                parent_id=parent.id,
                day=days[i]
            )
            new_schedule.save()

    else:
        new_account = america_account(
            first_name=cleanList[0],
            last_name=cleanList[1],
            phone_number="+%s" % (cleanList[6]),
            email_address=cleanList[7],
            stripe_customer_id=None,
            auth_type=0,
            active=True,
            profile_id=None,
            profile_type_id=None
        )
        new_account.save()

        new_parent = america_parent(
            account_id=new_account.id,
            primary_language=language,
            sms_preference_time=(datetime.strptime(cleanList[8], '%H%M') - timedelta(hours=3)).time()
        )
        new_parent.save()

        new_subscription = america_subscription(
            account_id=new_account.id
        )
        new_subscription.save()

        days = parse_days(cleanList[9])
        for i in range(len(days)):
            new_schedule = america_schedule(
                parent_id=new_parent.id,
                day=days[i]
            )
            new_schedule.save()

        bday = datetime.strptime(cleanList[4], '%m/%d/%Y')
        new_child = america_child(
            parent_id=new_parent.id,
            first_name=cleanList[2],
            last_name=cleanList[3],
            birthday=bday
        )
        new_child.save()

        today = date.today()
        age = (today.year - bday.year) - ((today.month, today.day) < (bday.month, bday.day))

        new_childprogress = america_childprogress(
            child_id=new_child.id,
            age=age,
            timestamp=datetime.utcnow()
        )
        new_childprogress.save()

print "Done"
