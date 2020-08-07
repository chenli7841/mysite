package extractHuobiOrderbook;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class SymbolInfo {

    public static Map<String, SymbolInfo> SYMBOL_INFO = ImmutableMap.of(
            "BTCUSDT", new SymbolInfo("USDT", "BTC", "market.btcusdt.depth.step0")
    );

    public String base;
    public String target;
    public String channel;

    public SymbolInfo(String base, String target, String channel) {
        this.base = base;
        this.target = target;
        this.channel = channel;
    }
}
