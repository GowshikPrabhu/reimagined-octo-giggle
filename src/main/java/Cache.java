import java.util.HashMap;
import java.util.Map;

public class Cache {
    private final Map<String, String> cache = new HashMap<>();

    public String get(String key) {
        return cache.get(key);
    }

    public void put(String key, String value) {
        cache.put(key, value);
    }
}
