const SYMBOL_INFO = {
	'BTCUSDT': { base: 'USDT', target: 'BTC', bitMexSymbol: 'XBTUSD' }
}

const args = process.argv.slice(2);
if (args.length != 1) {
  console.log('Please special a symbol. Such as node extractBitmexOrderbook/extractBitmexOrderbook.js BTCUSDT.');
  process.exit(1);
}
const inputSymbol = args[0];
const {base, target, bitMexSymbol} = SYMBOL_INFO[inputSymbol];

const BitMEXClient = require('bitmex-realtime-api');
const {BigQuery} = require('@google-cloud/bigquery');
const path = require('path');
const projectId = 'firebase-lispace';
const bigquery = new BigQuery({
  projectId: projectId,
  keyFilename: path.join(__dirname, '../credentials/bigquery.json')
});

const templateTableName = 'template';
const dataset = bigquery.dataset('bitmex_orderbooks');
const templateTable = dataset.table('template');
let currentTable = undefined;
let nextTable = undefined;
let lastPrice = undefined;
let lastRecord = undefined;

const client = new BitMEXClient({ testnet: false, maxTableLen: 1 });

if (new Date().getUTCHours() === 23) {
  const now = new Date();
  // At 11pm, create a new table ahead for the next day instead of doing it in the last second.
  templateTable.copy(dataset.table(NextDayString(now))).then(() => {
    nextTable = dataset.table(NextDayString(now));
  }).catch((e) => {
    console.log('Initially create next table failed', NextDayString(now), e);
    setTimeout(() => {
      nextTable = dataset.table(NextDayString(now));
    }, 5000);
  });
}

templateTable.copy(dataset.table(DateToString(new Date()))).then(() => {
  subscribe();
}).catch((e) => {
  console.log('Initially create current table failed', DateToString(new Date()), e);
  setTimeout(() => {
    subscribe();
  }, 5000);
});

function subscribe() {
  currentTable = dataset.table(DateToString(new Date()));
  client.addStream(bitMexSymbol, 'orderBook10', (data, _, table) => {
    if (data === undefined || data.length === 0) return;
    const {symbol, bids, asks, timestamp} = data[0];
    const t = new Date(timestamp);
    const record = {
      t: t,
      bid: bids[0][0],
      ask: asks[0][0],
      base: base,
      target: target,
      s: inputSymbol,
      localTime: new Date()
    };

    if (!lastRecord) {
      // initialization
      lastRecord = record;
      return;
    }
    if (lastRecord.t.getUTCHours() < 23 && t.getUTCHours() === 23) {
      // At 11pm, create a new table ahead for the next day instead of doing it in the last second.
      templateTable.copy(dataset.table(NextDayString(t))).then(() => {
        nextTable = dataset.table(NextDayString(t));
      }).catch((e) => {
        console.log('Create next table failed', NextDayString(t), e);
        setTimeout(() => {
          nextTable = dataset.table(NextDayString(t));
        }, 5000);
      });
    }
    if (lastRecord.t > record.t) {
      // FIFO violated, hopefully this is rare. May just log it in future.
      return;
    }
    const lastRecordInSeconds = TruncateToSeconds(lastRecord.t);
    const currRecordInSeconds = TruncateToSeconds(record.t);
    if (currRecordInSeconds - lastRecordInSeconds === 0) {
      // Still in the same seconds, not yet insert orderbook to BigQuery
      lastRecord = record;
      return;
    }
    if (lastRecordInSeconds.getDate() === currRecordInSeconds.getDate()) {
      // Different seconds, but in the same day, so just perform normal updates
      for (let i = lastRecordInSeconds; i < currRecordInSeconds; i = AddOneSecond(i)) {
        const toInsert = {
          insertId: AddOneSecond(i).toISOString(),
          json: {...record, t: AddOneSecond(i)}
        };
        currentTable.insert(toInsert, { raw: true });  
      }

      lastRecord = record;
      return;
    }
    // The remaining case is lastRecord and current Record not in the same date
    if (lastRecordInSeconds.getDate() !== currRecordInSeconds.getDate()) {
      // Assume no whole-day gap
      let i = lastRecordInSeconds;
      for (; !IsDayEnd(i); i = AddOneSecond(i)) {
        const toInsert = {
          insertId: AddOneSecond(i).toISOString(),
          json: {...record, t: AddOneSecond(i)}
        };
        currentTable.insert(toInsert, { raw: true });
      }
      currentTable = nextTable;
      nextTable = undefined;
      for (; i < currRecordInSeconds; i = AddOneSecond(i)) {
        const toInsert = {
          insertId: AddOneSecond(i).toISOString(),
          json: {...record, t: AddOneSecond(i)}
        };
        currentTable.insert(toInsert, { raw: true });
      }
      lastRecord = record;
      return;
    }
  });
}


function IsDayEnd(date) {
  return date.getUTCHours() === 23 && date.getUTCMinutes() === 59 && date.getUTCSeconds() === 59;
}

function AddOneSecond(date) {
  const copy = new Date(date);
  copy.setSeconds(copy.getSeconds()+1);
  return copy;
}

function TruncateToSeconds(date) {
  const copy = new Date(date);
  copy.setMilliseconds(0);
  return copy;
}

function DateToString(date) {
  return `${date.getUTCFullYear()}_${date.getUTCMonth() + 1}_${date.getUTCDate()}`;
}

function NextDayString(date) {
  const copy = new Date(date);
  copy.setDate(copy.getDate()+1);
  return `${copy.getUTCFullYear()}_${copy.getUTCMonth() + 1}_${copy.getUTCDate()}`;
}