'''
Reality:
The job can crash at any time
When it restarts, it re-reads the same input
Some input events are duplicates
Some are late-arriving
You must not double-count revenue
'''

from typing import Dict
from datetime import datetime
import json
import pickle
import os

class streaming_events:

    def __init__(self, events):
        self.daily_revenue = self.reading_file('daily_revenue.pkl', default={})
        self.events = events
        self.distinct_events = self.reading_file('distinct_events.pkl', default=set())

    def __enter__(self) -> "streaming_events":
        print("entering")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None: #write exceptions in exit block
        self.writing_file('daily_revenue.pkl',self.daily_revenue)
        self.writing_file('distinct_events.pkl', self.distinct_events)
        print("exiting")

    def writing_file(self, filename, content):
        with open(filename, 'wb') as f:
            pickle.dump(content, f)

    def reading_file(self, filename, default):
        if not os.path.exists(filename):
            return default
        try:
            with open(filename,'rb') as f:
                return pickle.load(f)
        except Exception:
            return default

    def event_parser(self, line):

        if not line:
            return None

        try:
            line_date, line_user, line_amount, line_currency = line.split()
            date = str(datetime.strptime(line_date, '%Y-%m-%d').date())
            user = line_user.split('=')[1]
            amount = float(line_amount.split('=')[1])
            currency = line_currency.split('=')[1]
        except Exception:
            print("incorrect row")
            return None

        if not (amount > 0 and currency == 'USD'):
            return None

        return date, user, amount, currency

    def process(self):

        for line in self.events:
            event = self.event_parser(line)

            if not event:
                continue

            if event in self.distinct_events:
                continue

            date, user, amount, currency = event
            self.distinct_events.add(event)

            if not date in self.daily_revenue:
                self.daily_revenue[date] = amount
                yield {'date': date, 'revenue': self.daily_revenue[date]}

            else:
                self.daily_revenue[date] += amount
                yield {'date': date, 'revenue': self.daily_revenue[date]}


input_events_2 = ["2024-02-07 user=99 amount=129.99 currency=USD"]

input_events = [
    # Day 1
    "2024-01-01 user=1 amount=9.99 currency=USD",
    "2024-01-01 user=2 amount=15.00 currency=USD",
    "2024-01-01 user=3 amount=5.00 currency=USD",
    "2024-01-01 user=1 amount=9.99 currency=USD",     # duplicate
    "2024-01-01 user=4 amount=free currency=USD",     # invalid
    "bad line",
    "",

    # Day 2
    "2024-01-02 user=1 amount=9.99 currency=USD",
    "2024-01-02 user=2 amount=10.00 currency=USD",
    "2024-01-02 user=2 amount=10.00 currency=USD",    # duplicate
    "2024-01-02 user=3 amount=-5 currency=USD",       # negative
    "2024-01-02 user=5 amount=7.50 currency=EUR",     # unsupported currency

    # Job crashes here 👇
    # ------------------------------------------------

    # Restart — SAME INPUT IS REPLAYED
    "2024-01-01 user=1 amount=9.99 currency=USD",
    "2024-01-01 user=2 amount=15.00 currency=USD",
    "2024-01-02 user=1 amount=9.99 currency=USD",
    "2024-01-02 user=2 amount=10.00 currency=USD",

    # New late-arriving data
    "2024-01-01 user=6 amount=20.00 currency=USD",
    "2024-01-02 user=6 amount=5.00 currency=USD",

    # Day 3
    "2024-01-03 user=1 amount=9.99 currency=USD",
    "2024-01-03 user=2 amount=9.99 currency=USD",
    "2024-01-03 user=1 amount=9.99 currency=USD",     # duplicate
]


with streaming_events(input_events_2) as m:

    for i in m.process():
        print(i)




print(m.__dict__)



































input_events = [
    # Day 1
    "2024-01-01 user=1 amount=9.99 currency=USD",
    "2024-01-01 user=2 amount=15.00 currency=USD",
    "2024-01-01 user=3 amount=5.00 currency=USD",
    "2024-01-01 user=1 amount=9.99 currency=USD",     # duplicate
    "2024-01-01 user=4 amount=free currency=USD",     # invalid
    "bad line",
    "",

    # Day 2
    "2024-01-02 user=1 amount=9.99 currency=USD",
    "2024-01-02 user=2 amount=10.00 currency=USD",
    "2024-01-02 user=2 amount=10.00 currency=USD",    # duplicate
    "2024-01-02 user=3 amount=-5 currency=USD",       # negative
    "2024-01-02 user=5 amount=7.50 currency=EUR",     # unsupported currency

    # Job crashes here 👇
    # ------------------------------------------------

    # Restart — SAME INPUT IS REPLAYED
    "2024-01-01 user=1 amount=9.99 currency=USD",
    "2024-01-01 user=2 amount=15.00 currency=USD",
    "2024-01-02 user=1 amount=9.99 currency=USD",
    "2024-01-02 user=2 amount=10.00 currency=USD",

    # New late-arriving data
    "2024-01-01 user=6 amount=20.00 currency=USD",
    "2024-01-02 user=6 amount=5.00 currency=USD",

    # Day 3
    "2024-01-03 user=1 amount=9.99 currency=USD",
    "2024-01-03 user=2 amount=9.99 currency=USD",
    "2024-01-03 user=1 amount=9.99 currency=USD",     # duplicate
]
