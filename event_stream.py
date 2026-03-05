from datetime import datetime
import json
import os

from stream_input import input_events

class event_stream:
    def __init__(self, events, filename):
        self.filename = filename
        self.state = self.store_state() # load state on init
        self.events = events[self.state['offset']:]

    def __enter__(self) -> "event_stream":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        print("exiting")

    def event_parser(self, data):
        if not data or data.count('=') != 3:
            return None

        data_date, data_user, data_amount, data_currency = data.split()

        try:
            date = datetime.strptime(data_date,'%Y-%m-%d').date()
            user = int(data_user.split("=")[1])
            amount = float(data_amount.split("=")[1])
            currency = data_currency.split("=")[1]
        except Exception:
            self.state['offset'] += 1 # also faulty input raises the offset.

            state = {'daily_revenue': self.state['daily_revenue'],
                     'processed_events': list(self.state['processed_events']),
                     'offset': self.state['offset']}

            self.json_dump(self.filename, state)

            raise ValueError(f'Event {data} does not meet the required format.')

        if amount < 0 or currency != 'USD':
            return None

        return str(date), user, amount, currency

    def validate_path(self, file_path):
        if not os.path.exists(file_path):
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            return False
        else:
            return True

    def json_dump(self, file, json_dict):
        with open(file, 'w') as j:
            json.dump(json_dict, j, indent=4)

    def json_load(self, file):
        with open(file, 'r') as j:
            return json.load(j)

    def store_state(self): # used on init

        if not self.validate_path(self.filename): #json non-existent, create file, fresh dict.
            self.json_dump(file = self.filename, json_dict = {'daily_revenue': {}, 'processed_events': [], 'offset': 0})
            return {'daily_revenue': {}, 'processed_events': set(), 'offset': 0}

        else: #json exists, read it.
            state = self.json_load(self.filename)
            return {'daily_revenue': state['daily_revenue'],
                        'processed_events': set(tuple(e) for e in state['processed_events']),
                        'offset': state['offset']}

    def set_state(self, event): # add the event to the state
        date, user, amount, currency = event  # unpack
        date = str(date)

        self.state['offset'] += 1 #for each call set_state: each event: the offset +1

        if len(self.state['daily_revenue']) == 0: #empty state
            self.state['daily_revenue'][str(date)] = amount
            self.state['processed_events'].add(event)

        else:
            if event in self.state['processed_events']: # duplicate event
                return None
            elif not str(date) in self.state['daily_revenue']: # new date
                self.state['daily_revenue'][str(date)] = amount
                self.state['processed_events'].add(event)
            else: # existing date
                self.state['daily_revenue'][str(date)] += amount
                self.state['processed_events'].add(event)

        self.write_state()
        return self.state

    def write_state(self):

        state = {'daily_revenue': self.state['daily_revenue'],
                 'processed_events':  list(self.state['processed_events']),
                 'offset': self.state['offset']}

        try:
            self.json_dump(self.filename, state)
        except Exception:
            raise ValueError(f'State {state} not json dumped.')

    def stream_process(self):

        for line in self.events:

            event = self.event_parser(line)

            if not event:
                continue

            self.set_state(event)

            yield {'daily_revenue': self.state['daily_revenue'][str(event[0])]}



with event_stream(input_events,'../output/event_stream.json') as m:

    for i in m.stream_process():
        print(i)




