package extractHuobiOrderbook;

public class Orderbook extends DataFrame {

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
