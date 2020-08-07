package extractHuobiOrderbook;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;

import static extractHuobiOrderbook.SymbolInfo.SYMBOL_INFO;

public class HuobiClient extends WebSocketClient {

    private Gson gson = new Gson();

    private OrderbookMapper mapper = new OrderbookMapper();

    private String symbol;

    private Consumer<Map<String, ?>> onOrderbook;

    private Consumer<Object> onDisconnect;

    public HuobiClient(URI serverUri, String symbol, Consumer<Map<String, ?>> onOrderbook, Consumer onDisconnect) {
        super(serverUri);
        this.symbol = symbol;
        this.onOrderbook = onOrderbook;
        this.onDisconnect = onDisconnect;
    }

    @Override
    public void onOpen(ServerHandshake handshakedata) {
        System.out.println("opened connection");
    }

    @Override
    public void onMessage(String message) {
        System.out.println("received String message? " + message);
    }

    @Override
    public void onMessage(ByteBuffer bytes) {
        try {
            processMessage(bytes);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        System.out.println( "Closed by " + ( remote ? "remote" : "us" ) +
                " Code: " + code +
                " Reason: " + reason + ", " +
                new DateTime() + ", " +
                this.hashCode() + ", " +
                "restarting? " + (this.onDisconnect != null));
        if (onDisconnect != null) {
            onDisconnect.accept(null);
        }
    }

    public void dispose() {
        this.onDisconnect = null;
        this.close();
    }

    @Override
    public void onError( Exception ex ) {
        ex.printStackTrace();
        // if the error is fatal then onClose will be called additionally
    }

    private void processMessage(final ByteBuffer msg) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(msg.array());
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
                this.send(pong);
            } else {
                System.out.println("Unknown message: " + sb.toString());
            }
        } catch (JsonSyntaxException ex) {
            System.out.println("Json parse exception");
            System.out.println(ex.getMessage());
        }
    }
}
