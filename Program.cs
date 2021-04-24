using MoreLinq;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using RestSharp;
using Serilog;
using System;
using System.Linq;
using System.Collections.Generic;
using System.IO;
using System.Security.Authentication;
using System.Threading;
using WebSocketSharp;

namespace Example
{
    class Program
    {
        private const string urlGetInstruments = "https://test.deribit.com/api/v2/public/get_instruments";

        //put in file config
        private const string urlSocket = "wss://test.deribit.com/ws/api/v2";

        //this should not be hardcoded - various options...
        private static readonly string[] currencies = new [] { 
            "BTC", 
            "ETH" 
        };

        //could make this configurable
        private static readonly (string outter, string inner)[] paths = new []
        {
            (outter: "params", inner: "data"),
            (outter: "result", inner: "trades")
        };

        private static void Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
               .WriteTo.Console()
               .CreateLogger();

            try
            {
                using var ws = new WebSocket(urlSocket);
                ws.SslConfiguration.EnabledSslProtocols = SslProtocols.Tls12;
                ws.OnMessage += (sender, e) => { if (e.IsText) HandleMessage(e.Data); };
                ws.OnError += (sender, e) => { Log.Error(e.Message); };
                ws.Connect();

                foreach (var ccy in currencies)
                {
                    ws.Send(JsonConvert.SerializeObject(new
                    {
                        jsonrpc = "2.0",
                        id = 0,
                        method = "public/get_last_trades_by_instrument",
                        @params = new { instrument_name = $"{ccy}-PERPETUAL" }
                    }));
                    ws.Send(JsonConvert.SerializeObject(new
                    {
                        jsonrpc = "2.0",
                        id = 1,
                        method = "public/subscribe",
                        @params = new { channels = GetInstruments(ccy: ccy, expired: false, kind: "future") }
                    }));
                }

                Thread.CurrentThread.Join();
            }
            catch (Exception e)
            {
                Log.Error(e.Message);
            }
        }

        public static string[] GetInstruments(string ccy, bool expired, string kind)
        {
            var parameters = new Dictionary<string, string>()
            {
                ["currency"] = ccy,
                ["expired"] = expired.ToString().ToLower(),
                ["kind"] = kind
            };
            var client = new RestClient(urlGetInstruments);
            var request = new RestRequest(Method.GET);
            request.AddHeader("Content-Type", "application/json");
            parameters.ForEach(x => request.AddParameter(x.Key, x.Value));
            return JObject.Parse(client.Execute(request).Content)
                .SelectToken("result")
                ?.Select(x => $"trades.{x.SelectToken("instrument_name")}.raw")
                .ToArray();
        }

        private static void HandleMessage(string Data)
        {
            var obj = JObject.Parse(Data);
            paths.ForEach(p => obj
                .SelectToken(p.outter)
                ?.SelectToken(p.inner)
                ?.OrderBy(x => (string)x["timestamp"])
                .ThenBy(x => (string)x["trade_seq"])
                .ForEach(x => {
                    var t = ParseTrade(x);
                    PrintTrade(t);
                    //add more methods here for KDB, a message queue, and/or a grid
                })
            );
        }

        private static Trade ParseTrade(JToken trade)
        {
            return new Trade
            {
                trade_seq = (string)trade["trade_seq"],
                trade_id = (string)trade.SelectToken("trade_id"), // this field is not included in some messages
                timestamp = (string)trade["timestamp"],
                tick_direction = (string)trade["tick_direction"],
                mark_price = (string)trade["mark_price"],
                price = (string)trade["price"],
                instrument_name = (string)trade["instrument_name"],
                index_price = (string)trade["index_price"],
                direction = (string)trade["direction"],
                amount = (string)trade["amount"]
            };
        }

        private static void PrintTrade(Trade t)
        {
            var path = Path.Combine(Path.GetTempPath(), $"Trades_{DateTime.Today:yyyyMMdd}.csv"); //start new file at midnight
            using var writer = new StreamWriter(path, true);
            if (new FileInfo(path).Length == 0)
                writer.WriteLine("trade_seq,trade_id,timestamp,tick_direction,mark_price,price,instrument_name,index_price,direction,amount");
            writer.WriteLine($"{t.trade_seq},{t.trade_id},{t.timestamp},{t.tick_direction},{t.mark_price},{t.price},\"{t.instrument_name}\",{t.index_price},{t.direction},{t.amount}");
        }
    }

    class Trade
    {
        internal string trade_seq { get; set; }
        internal string trade_id { get; set; }
        internal string timestamp { get; set; }
        internal string tick_direction { get; set; }
        internal string mark_price { get; set; }
        internal string price { get; set; }
        internal string instrument_name { get; set; }
        internal string index_price { get; set; }
        internal string direction { get; set; }
        internal string amount { get; set; }
    }
} 