using System;
using System.Collections.Generic;
using System.Linq;
using Binance.Net;
using Google.Cloud.BigQuery.V2;
using Google.Apis.Bigquery.v2.Data;
using Google.Apis.Auth.OAuth2;
using Binance.Net.Objects.Spot.MarketData;

namespace extractBinance
{

    public class SymbolInfo
    {
        public string targetCurrency;

        public string baseCurrency;
    }

    public class RecordOrderbook
    {
        readonly BinanceSocketClient socketClient;
        readonly BigQueryClient bqClient;
        readonly BinanceClient binanceClient;
        readonly string symbol;
        BinanceOrderBook lastRecord = null;
        static readonly Dictionary<string, SymbolInfo> SYMBOL_MAP = new Dictionary<string, SymbolInfo>() {
            { "BTCUSDT", new SymbolInfo() { baseCurrency = "USDT", targetCurrency = "BTC" }}
        };
        public RecordOrderbook(string symbol)
        {
            if (symbol == null || symbol.Trim() == "")
            {
                throw new MissingFieldException("Please specify a symbol");
            }
            this.symbol = symbol;
            var credential = GoogleCredential.FromFile("./credentials/bigquery.json");
            this.bqClient = BigQueryClient.Create("firebase-lispace", credential);
            this.socketClient = new BinanceSocketClient();
            this.binanceClient = new BinanceClient();
        }
        public void execute()
        {
            setupTable(DateTime.UtcNow);
            
            var conn = socketClient.SubscribeToOrderBookUpdates(this.symbol, 1000, (data) => 
            {
                DateTime now = DateTime.UtcNow;
                insertRow(data);
            });
        }

        private void insertRow(BinanceOrderBook data)
        {
            if (lastRecord == null)
            {
                lastRecord = data;
                return;
            }
            DateTime currTime = data.EventTime.Value.ToUniversalTime();
            DateTime prevTime = lastRecord.EventTime.Value.ToUniversalTime();
            DateTime currSecond = truncateToSecond(currTime);
            DateTime prevSecond = truncateToSecond(prevTime);
            if (lastRecord.EventTime.Value > data.EventTime.Value)
            {
                return;
            }
            else if (prevSecond == currSecond)
            {
                lastRecord = data;
                return;
            }
            else if (prevTime.Day == currTime.Day)
            {
                for (DateTime s = prevSecond; s < currSecond; s = addOneSecond(s))
                {
                    try
                    {
                        insertToBigQuery(s, lastRecord);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.ToString());
                    }
                }
                Console.WriteLine("Prev Day: " + prevTime + ", Curr Day: " + currTime);
                lastRecord = data;
                return;
            }
            else
            {
                setupTable(currTime);
                for (DateTime s = prevSecond; s < currSecond; s = addOneSecond(s))
                {
                    insertToBigQuery(s, lastRecord);
                }
                lastRecord = data;
                return;
            }
        }

        private void insertToBigQuery(DateTime currSecond, BinanceOrderBook record)
        {
            string baseCurrency = SYMBOL_MAP[symbol].baseCurrency;
            string targetCurrency = SYMBOL_MAP[symbol].targetCurrency;

            BigQueryInsertRow row = new BigQueryInsertRow(insertId: currSecond.ToString())
            {
                { "t", addOneSecond(currSecond) },
                { "bid", BigQueryNumeric.Parse(record.Bids.ToArray()[0].Price.ToString()) },
                { "ask", BigQueryNumeric.Parse(record.Asks.ToArray()[0].Price.ToString()) },
                { "base", baseCurrency },
                { "target", targetCurrency },
                { "s", symbol },
                { "localTime", DateTime.UtcNow }
            };
            bqClient.InsertRow("firebase-lispace", "binance_orderbooks", getTableName(addOneSecond(currSecond)), row);
        }

        private void setupTable(DateTime dateTime)
        {
            DateTime now = DateTime.UtcNow;
            TableReference src = new TableReference()
            {
                ProjectId = "firebase-lispace",
                DatasetId = "binance_orderbooks",
                TableId = "template"
            };
            TableReference dst = new TableReference()
            {
                ProjectId = "firebase-lispace",
                DatasetId = "binance_orderbooks",
                TableId = getTableName(dateTime)
            };
            bqClient.CreateCopyJob(src, dst);
        }

        private DateTime truncateToSecond(DateTime time)
        {
            DateTime utc = time.ToUniversalTime();
            utc = utc.AddMilliseconds(-utc.Millisecond);
            return utc;
        }

        private DateTime addOneSecond(DateTime time)
        {
            return time.AddSeconds(1);
        }

        private string getTableName(DateTime date)
        {
            return $"{date.Year}_{date.Month}_{date.Day}";
        }
    }
}