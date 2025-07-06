import java.util.*;

public class Cache {
    private final Map<String, String> data = new HashMap<>();
    private final Map<String, Long> expirations = new HashMap<>();

    public String get(String key) {
        if (isExpired(key)) {
            data.remove(key);
            expirations.remove(key);
            return null;
        }
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

    private boolean isExpired(String key) {
        Long expiration = expirations.get(key);
        if (expiration == null) {
            return false;
        }
        return System.currentTimeMillis() >= expiration;
    }

    public String[] keys() {
        return data.keySet().toArray(new String[0]);
    }
}
