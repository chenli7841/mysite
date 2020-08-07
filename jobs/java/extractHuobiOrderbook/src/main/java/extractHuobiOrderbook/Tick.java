package extractHuobiOrderbook;

import java.math.BigDecimal;

public class Tick {
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
