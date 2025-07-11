import java.util.HashMap;
import java.util.Map;

public class Configs {
    public static final long EXPIRY_SCAN_INTERVAL_MS = 100;
    public static final long SELECTOR_WAIT_INTERVAL_MS = 10;
    public static final int READ_BUFFER_SIZE = 8196;
    public static final int MAX_COMMANDS_PER_READ = 100;

    private static final Map<String, String> config = new HashMap<>();

    private static final Map<String, String> replicationInfo = new HashMap<>();

    public static String getConfiguration(String key) {
        return config.get(key);
    }

    public static void setConfiguration(String key, String value) {
        config.put(key, value);
    }

    public static String getReplicationInfo(String key) {
        return replicationInfo.get(key);
    }

    public static void setReplicationInfo(String key, String value) {
        replicationInfo.put(key, value);
    }

    static {
        replicationInfo.put("role", "master");
    }
}
