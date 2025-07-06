import java.util.Iterator;
import java.util.Map;

public class Expiry {
    private final Cache cache = Cache.getInstance();
    private long lastScan = 0L;

    public void scanAndExpire() {
        long now = System.currentTimeMillis();
        if ((now - lastScan) < Configs.EXPIRY_SCAN_INTERVAL_MS) {
            return;
        }
        lastScan = now;

        Iterator<Map.Entry<String, Long>> it = cache.expirableIterator();
        while (it.hasNext()) {
            Map.Entry<String, Long> entry = it.next();
            if (entry.getValue() <= now) {
                cache.expired(entry.getKey());
                it.remove();
            }
        }
    }
}
