import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Cache {
    private final Map<String, String> data = new HashMap<>();
    private final Map<String, Long> expirations = new HashMap<>();

    public String get(String key) {
        return data.get(key);
    }

    public void put(String key, String value, long ttlMillis) {
        data.put(key, value);
        if (ttlMillis > 0) {
            expirations.put(key, System.currentTimeMillis() + ttlMillis);
        } else {
            expirations.remove(key);
        }
    }

    public Iterator<Map.Entry<String, Long>> expirableIterator() {
        return expirations.entrySet().iterator();
    }

    public void expired(String key) {
        data.remove(key);
    }
}
