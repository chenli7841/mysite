import sys
from copy import deepcopy
# terrible...python datetime has no zone info
from datetime import datetime, timedelta
from typing import Optional, Dict
import json
import websocket
import zlib
from google.cloud import bigquery
from google.oauth2 import service_account

SYMBOL_MAP = {
    'BTCUSDT': {'base': 'USDT', 'target': 'BTC', 'channel': 'spot/depth5:BTC-USDT'}
}

if len(sys.argv) != 2:
    print('Please specify a symbol. For example, python extractOkexOrderbook.py BTCUSDT')
    exit(1)

symbol = sys.argv[1]

if sys.argv[1] not in SYMBOL_MAP:
    print(f'The symbol {sys.argv[1]} cannot be found. Please specify a different one.')
    exit(1)


class Record(object):
    def __init__(self, t: datetime, bid: float, ask: float, base: str, target: str, s: str, local_time: datetime):
        self.t = t
        self.bid = bid
        self.ask = ask
        self.base = base
        self.target = target
        self.s = s
        self.localTime = local_time


last_record: Optional[Record] = None


def date_to_string(utc_date: datetime) -> str:
    return f'{utc_date.year}_{utc_date.month}_{utc_date.day}'


credentials = service_account.Credentials.from_service_account_file(
    'bigquery.json',
    scopes=["https://www.googleapis.com/auth/cloud-platform"])

client = bigquery.Client(credentials=credentials, project=credentials.project_id)


try:
    client.copy_table(
        'firebase-lispace.okex_orderbooks.template',
        f'firebase-lispace.okex_orderbooks.{date_to_string(datetime.utcnow())}').result()
except Exception as e:
    print(e)

curr_table = client.get_table(f'firebase-lispace.okex_orderbooks.{date_to_string(datetime.utcnow())}')


def get_record(obj_dict: Dict, base: str, target: str, s: str) -> Record:
    record = Record(
        datetime.strptime(obj_dict['data'][0]['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ'),
        float(obj_dict['data'][0]['bids'][0][0]),
        float(obj_dict['data'][0]['asks'][0][0]),
        base,
        target,
        s,
        datetime.utcnow()
    )
    return record


def truncate_to_second(dt: datetime) -> datetime:
    return datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second)


def get_datetime_string(dt: datetime) -> str:
    return f'{dt.year}_{dt.month}_{dt.day}_{dt.hour}_{dt.minute}_{dt.second}'


def get_day_end(dt: datetime) -> datetime:
    return datetime(dt.year, dt.month, dt.day, 23, 59, 59)


def on_message(ws, message):
    data = zlib.decompress(message, -zlib.MAX_WBITS)
    obj = json.loads(data)

    global last_record

    curr_record = get_record(
        obj,
        SYMBOL_MAP[symbol]['base'],
        SYMBOL_MAP[symbol]['target'],
        symbol
    )
    global curr_table

    if last_record is None:
        last_record = curr_record
    elif last_record.t.second == curr_record.t.second:
        last_record = curr_record
    elif last_record.t.day == curr_record.t.day:
        curr = truncate_to_second(last_record.t)
        until = truncate_to_second(curr_record.t)
        # E.g., last_record is in 19.333 sec, curr_record is 20.555 sec
        while curr < until:
            to_insert = deepcopy(last_record)
            to_insert.t = (curr + timedelta(0, 1)).isoformat()
            to_insert.localTime = to_insert.localTime.isoformat()
            try:
                client.insert_rows_json(
                    curr_table,
                    [to_insert.__dict__],
                    [get_datetime_string(curr + timedelta(0, 1))]
                )
            except Exception as e:
                print(e)
            curr = curr + timedelta(0, 1)
        last_record = curr_record
    else:
        curr = truncate_to_second(last_record.t)
        until = truncate_to_second(curr_record.t)
        day_end = get_day_end(last_record.t)
        while curr < day_end:
            to_insert = deepcopy(last_record)
            to_insert.t = (curr + timedelta(0, 1)).isoformat()
            to_insert.localTime = to_insert.localTime.isoformat()
            try:
                client.insert_rows_json(
                    curr_table,
                    [to_insert.__dict__],
                    [get_datetime_string(curr + timedelta(0, 1))]
                )
            except Exception as e:
                print(e)
            curr = curr + timedelta(0, 1)
        try:
            client.copy_table(
                'firebase-lispace.okex_orderbooks.template',
                f'firebase-lispace.okex_orderbooks.{date_to_string(curr_record.t)}').result()
        except Exception as e:
            print(e)
        curr_table = client.get_table(f'firebase-lispace.okex_orderbooks.{date_to_string(curr_record.t)}')
        while curr < until:
            to_insert = deepcopy(last_record)
            to_insert.t = curr + timedelta(0, 1)
            try:
                client.insert_rows_json(
                    curr_table,
                    [to_insert.__dict__],
                    [get_datetime_string(to_insert.t)]
                )
            except Exception as e:
                print(e)
            curr = curr + timedelta(0, 1)
        last_record = curr_record


def on_error(ws, error):
    print(error)


def on_close(ws):
    print("### closed ###")


def on_open(ws):
    ws.send('{"op": "subscribe", "args": ["spot/depth5:BTC-USDT"]}')


ws = websocket.WebSocketApp("wss://real.okex.com:8443/ws/v3",
                            on_message=on_message,
                            on_error=on_error,
                            on_close=on_close)
ws.on_open = on_open
ws.run_forever()
