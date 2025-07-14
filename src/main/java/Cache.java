import java.util.*;

public class Cache {
    private static volatile Cache instance;

    private final Map<String, Value> data;
    private final Map<String, Long> expirations;

    public static final String TYPE_STRING = "string";
    public static final String TYPE_STREAM = "stream";

    private Cache() {
        this(128, 32);
    }

    private Cache(int dataSize, int expireDBSize) {
        data = new HashMap<>(dataSize);
        expirations = new HashMap<>(expireDBSize);
    }

    public Value get(String key) {
        if (isExpired(key)) {
            data.remove(key);
            expirations.remove(key);
            return null;
        }
        return data.get(key);
    }

    public void put(String key, Value value, long ttlMillis) {
        data.put(key, value);
        if (ttlMillis > 0) {
            expirations.put(key, System.currentTimeMillis() + ttlMillis);
        } else {
            expirations.remove(key);
        }
    }

    public void putFromDB(String key, Value value, long timeStampMillis) {
        data.put(key, value);
        if (timeStampMillis > 0) {
            expirations.put(key, timeStampMillis);
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

    public static Cache getInstance() {
        if (instance == null) {
            synchronized (Cache.class) {
                if (instance == null) {
                    instance = new Cache();
                }
            }
        }
        return instance;
    }

    public static Cache getInstance(int dataSize, int expireDBSize) {
        if (instance == null) {
            synchronized (Cache.class) {
                if (instance == null) {
                    instance = new Cache(dataSize, expireDBSize);
                }
            }
        }
        return instance;
    }

    public static class Value {
        private final Object value;
        private final String type;

        public Value(Object value, String type) {
            this.value = value;
            this.type = type;
        }

        public Object getValue() {
            return value;
        }

        public String getType() {
            return type;
        }
    }
}
