package extractHuobiOrderbook;

import org.joda.time.DateTime;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Timer;
import java.util.TimerTask;

import static extractHuobiOrderbook.SymbolInfo.SYMBOL_INFO;

public class App {

    private static HuobiClient oldClient = null;
    private static HuobiClient newClient = null;
    private static InsertJob job = null;
    private static String symbol = null;

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.out.println("Please enter 1 argument, such as java -jar ./build/libs/extractHuobiOrderbook-all.jar BTCUSDT");
            return;
        }
        job = new InsertJob();

        symbol = args[0];
        Timer connectTimer = new Timer();
        final TimerTask connectTask = new TimerTask() {
            @Override
            public void run() {
                connect();
            }
        };
        TimerTask disconnectTask = new TimerTask() {
            @Override
            public void run() {
                System.out.println("disconnect, " + new DateTime().toString());
                if (oldClient != null) {
                    oldClient.dispose();
                    oldClient = null;
                }
            }
        };
        connectTask.run();
        connectTimer.schedule(connectTask, 1000 * (60 * 29 + 50), 1000 * 60 * 30);
        connectTimer.schedule(disconnectTask, 1000 * 60 * 30, 1000 * 60 * 30);
    }

    private static void connect() {
        System.out.println("connect, " + new DateTime().toString());
        oldClient = newClient;
        try {
            newClient = new HuobiClient(new URI("wss://api.huobi.pro/ws"), "BTCUSDT", job::insertRow, (obj) -> connect());
        } catch (URISyntaxException ex) {
            ex.printStackTrace();
            return;
        }
        try {
            newClient.connectBlocking();
        } catch (InterruptedException ex) {
            System.out.println("Connect error...");
            ex.printStackTrace();
            return;
        }
        final String channel = SYMBOL_INFO.get(symbol).channel;
        final String req = "{\"sub\": \"" + channel + "\", \"id\": \"id1\"}";
        newClient.send(req);
    }
}
