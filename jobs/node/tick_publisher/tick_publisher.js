const BitMEXClient = require('bitmex-realtime-api');
const amqp = require('amqplib');
const express = require('express');
const url = require('url');
const registerSwaggerUi = require('./../helpers').registerSwaggerUi;

const e = 'tickPublisher';

const cachedPrice = {};

// Publisher
async function publish() {
  const conn = await amqp.connect('amqp://localhost');
  const ch = await conn.createChannel();
  await ch.assertExchange(e, 'topic');
  const client = new BitMEXClient({ testnet: true, maxTableLen: 1 });
  client.addStream('*', 'trade', (data, symbol, table) => {
    if (data === undefined) return;
    const tick = data[0];
    if (tick === undefined) return;
    cachedPrice[tick.symbol] = {
      timestamp: tick.timestamp,
      side: tick.side,
      price: tick.price,
      direction: tick.tickDirection,
      trdMatchID: tick.trdMatchID,
      grossValue: tick.grossValue,
	  homeNotional: tick.homeNotional,
	  foreignNotional: tick.foreignNotional
    };
    ch.publish(e, '', Buffer.from(JSON.stringify(tick)));
  });
}

function startServer() {
  const app = express();
  app.listen(5001);
  app.route('/v1/tick').get(function(req, res) {
	let urlQuery = url.parse(req.url, true).query;
    let symbol = urlQuery.symbol;
    if (symbol === undefined) {
      res.send('Please specify a symbol.');
      return;
    }

    const record = cachedPrice[symbol];

    if (record === undefined) {
      res.status(400).send('The symbol ' + symbol + ' does not exist.');
      return;
    }

    res.send(record);
  });
  app.route('/v1/symbols/list').get(function(req, res) {
	const symbols = Object.keys(cachedPrice);
	res.send(symbols.filter(s => s.indexOf('.') === -1));
  });
  registerSwaggerUi(
    './tick_publisher/tick_publisher.yml',
	'http://localhost:5001/v1',
	app
  );
}

publish();
startServer();