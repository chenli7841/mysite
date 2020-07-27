package extractHuobiOrderbook;

import javax.websocket.ContainerProvider;
import javax.websocket.DeploymentException;
import javax.websocket.WebSocketContainer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

public class App {
    public static void main(String[] args) {
        try {
            final WebSocketContainer container = ContainerProvider.getWebSocketContainer();
            final String uri = "wss://api.huobi.pro/ws";
            container.connectToServer(HuobiClientEndpoint.class, URI.create(uri));
            BufferedReader r= new BufferedReader(new InputStreamReader(System.in));
            while(true){
                String line=r.readLine();
                if(line.equals("quit")) break;
            }

        } catch (IOException | DeploymentException ex) {
            ex.printStackTrace();;
        }
    }
}
