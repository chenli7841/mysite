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
    public class RecordOrderbook
    {
        int? currentDay = null;
        int lastSecond = -1;
        readonly BinanceSocketClient socketClient;
        readonly BigQueryClient bqClient;
        readonly BinanceClient binanceClient;
        readonly string symbol;
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

            //BinanceOrderBook data = binanceClient.GetOrderBook("BTCUSDT").Data;
            //insertRow(data, getDateText(now));
            
            socketClient.SubscribeToOrderBookUpdates("BTCUSDT", 1000, (data) => 
            {
                DateTime now = DateTime.UtcNow;
                if (currentDay == null || now.Day != currentDay)
                {
                    setupTable();
                    currentDay = now.Day;
                }
                insertRow(data, getDateText(now));
            });
        }

        private void insertRow(BinanceOrderBook data, string tableName)
        {
            DateTime? time = null;
            if (data.EventTime.HasValue)
            {
                time = data.EventTime.Value.Second == lastSecond ?
                    data.EventTime.Value : getTime(data.EventTime.Value);
                lastSecond = data.EventTime.Value.Second;
            }
            
            BigQueryInsertRow row = new BigQueryInsertRow(insertId: time == null ? null : time.ToString())
            {
                { "t", data.EventTime },
                { "bid", BigQueryNumeric.Parse(data.Bids.ToArray()[0].Price.ToString()) },
                { "ask", BigQueryNumeric.Parse(data.Asks.ToArray()[0].Price.ToString()) },
                { "base", "USDT" },
                { "target", "BTC" },
                { "s", "BTCUSDT" },
                { "localTime", DateTime.UtcNow }
            };
            bqClient.InsertRow("firebase-lispace", "binance_orderbooks", tableName, row);
        }

        private void setupTable()
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
                TableId = getDateText(now)
            };
            bqClient.CreateCopyJob(src, dst);
        }

        private DateTime getTime(DateTime time)
        {
            DateTime utc = time.ToUniversalTime();
            utc = utc.AddMilliseconds(-utc.Millisecond);
            return utc;
        }

        private string getDateText(DateTime date)
        {
            return $"{date.Year}_{date.Month}_{date.Day}";
        }
    }
}