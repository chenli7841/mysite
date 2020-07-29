package extractHuobiOrderbook;

import javax.websocket.ContainerProvider;
import javax.websocket.DeploymentException;
import javax.websocket.WebSocketContainer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

public class App {
    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.out.println("Please enter 1 argument, such as java -jar ./build/libs/extractHuobiOrderbook-all.jar BTCUSDT");
            return;
        }
        final InsertJob job = new InsertJob();
        try {
            final WebSocketContainer container = ContainerProvider.getWebSocketContainer();
            final String uri = "wss://api.huobi.pro/ws";
            HuobiClientEndpoint client = new HuobiClientEndpoint(args[0], job::insertRow);
            container.connectToServer(client, URI.create(uri));
            BufferedReader r= new BufferedReader(new InputStreamReader(System.in));
            while(true){
                String line=r.readLine();
                if(line != null || line.equals("quit")) break;
            }

        } catch (IOException | DeploymentException ex) {
            ex.printStackTrace();
        }
    }
}
