package extractHuobiOrderbook;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.Map;

public class OrderbookMapper {
    public Map<String, String> map(final Orderbook orderbook, final String symbol, final String base, final String target) {
        return Map.of(
                "t", new DateTime().withMillis(orderbook.getTs()).withZone(DateTimeZone.UTC).toString(),
                "ask", orderbook.getTick().getAsks()[0][0].toString(),
                "bid", orderbook.getTick().getBids()[0][0].toString(),
                "base", base,
                "target", target,
                "s", symbol,
                "localTime", DateTime.now(DateTimeZone.UTC).toString()
        );
    }
}
