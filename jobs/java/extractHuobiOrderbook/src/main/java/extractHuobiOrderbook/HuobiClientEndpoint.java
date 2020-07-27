package extractHuobiOrderbook;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import javax.websocket.*;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
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
        private Tick tick;

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

    Gson gson = new Gson();

    @OnOpen
    public void onOpen(final Session session) throws IOException {
        final String req = "{\"sub\": \"market.btcusdt.depth.step0\", \"id\": \"id1\"}";
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
        System.out.println(sb.toString());
        try {
            Orderbook ob = gson.fromJson(sb.toString(), Orderbook.class);
            if (ob.getTick() != null) {
                System.out.println(ob.getTick().getAsks()[0][0]);
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
        /*
        try {
            PingMessage ping = gson.fromJson(sb.toString(), PingMessage.class);
            System.out.println(ping.getPing());
        } catch (JsonSyntaxException ex) {
            System.out.println("Json parse exception");
            System.out.println(ex.getMessage());
        }
         */
    }

    @OnMessage
    public void pongMessage(Session session, PongMessage msg) {
        System.out.println("pong message " + msg.toString());
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
