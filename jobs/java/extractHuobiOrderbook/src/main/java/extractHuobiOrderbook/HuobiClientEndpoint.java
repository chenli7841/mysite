package extractHuobiOrderbook;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import javax.websocket.*;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;

@ClientEndpoint
public class HuobiClientEndpoint {

    private static class DataFrame {
        private Long ping;

        public Long getPing() {
            return ping;
        }

        public void setPing(Long ping) {
            this.ping = ping;
        }
    }

    private static class Orderbook extends DataFrame {

        private long ts;

        private Tick tick;

        public long getTs() {
            return ts;
        }

        public void setTs(long ts) {
            this.ts = ts;
        }

        public Tick getTick() {
            return tick;
        }

        public void setTick(Tick tick) {
            this.tick = tick;
        }
    }

    private static class Tick {
        private BigDecimal[][] bids;
        private BigDecimal[][] asks;

        public BigDecimal[][] getBids() {
            return bids;
        }

        public void setBids(BigDecimal[][] bids) {
            this.bids = bids;
        }

        public BigDecimal[][] getAsks() {
            return asks;
        }

        public void setAsks(BigDecimal[][] asks) {
            this.asks = asks;
        }
    }

    private static class OrderbookMapper {
        public Map<String, ?> map(final Orderbook orderbook, final String symbol, final String base, final String target) {
            return Map.of(
                    "t", new DateTime().withMillis(orderbook.ts).withZone(DateTimeZone.UTC).toString(),
                    "ask", orderbook.tick.asks[0][0],
                    "bid", orderbook.tick.bids[0][0],
                    "base", base,
                    "target", target,
                    "s", symbol,
                    "localTime", DateTime.now(DateTimeZone.UTC).toString()
            );
        }
    }

    private static class SymbolInfo {
        public String base;
        public String target;
        public String channel;

        public SymbolInfo(String base, String target, String channel) {
            this.base = base;
            this.target = target;
            this.channel = channel;
        }
    }

    private Gson gson = new Gson();

    private OrderbookMapper mapper = new OrderbookMapper();

    private static Map<String, SymbolInfo> SYMBOL_INFO = ImmutableMap.of(
            "BTCUSDT", new SymbolInfo("USDT", "BTC", "market.btcusdt.depth.step0")
    );

    private String symbol;
    private Consumer<Map<String, ?>> onOrderbook;

    public HuobiClientEndpoint(final String symbol, final Consumer<Map<String, ?>> onOrderbook) {
        this.symbol = symbol;
        this.onOrderbook = onOrderbook;
    }

    @OnOpen
    public void onOpen(final Session session) throws IOException {
        final String channel = SYMBOL_INFO.get(symbol).channel;
        final String req = "{\"sub\": \"" + channel + "\", \"id\": \"id1\"}";
        System.out.println("sent req on open");
        session.getBasicRemote().sendText(req);
    }

    @OnMessage
    public void processMessage(byte[] msg, final Session session, boolean last) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(msg);
        GZIPInputStream gis = new GZIPInputStream(bis);
        BufferedReader br = new BufferedReader(new InputStreamReader(gis, StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder();
        String line;
        while((line = br.readLine()) != null) {
            sb.append(line);
        }
        br.close();
        gis.close();
        bis.close();
        try {
            Orderbook ob = gson.fromJson(sb.toString(), Orderbook.class);
            if (ob.getTick() != null) {
                final SymbolInfo symbol_info = SYMBOL_INFO.get(symbol);
                final Map<String, ?> record = mapper.map(ob, symbol, symbol_info.base, symbol_info.target);
                if (onOrderbook != null) {
                    onOrderbook.accept(record);
                }
            } else if (ob.getPing() != null) {
                String pong = "{\"pong\":" + ob.getPing() + "}";
                session.getBasicRemote().sendText(pong);
            } else {
                System.out.println("Unknown message");
            }
        } catch (JsonSyntaxException ex) {
            System.out.println("Json parse exception");
            System.out.println(ex.getMessage());
        }
    }

    @OnError
    public void processError(Throwable t) {
        t.printStackTrace();
    }

    @OnClose
    public void processClose() {
        System.out.println("The websocket connection is closed.");
    }
}
