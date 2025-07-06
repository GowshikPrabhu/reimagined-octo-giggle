import java.util.List;
import java.util.Map;

public class RESPEncoder {

    public static String encodeSimpleString(String s) {
        return "+" + s + "\r\n";
    }

    public static String encodeInteger(long num) {
        return ":" + num + "\r\n";
    }

    public static String encodeBulkString(String s) {
        if (s == null) {
            return "$-1\r\n";
        }
        return "$" + s.length() + "\r\n" + s + "\r\n";
    }

    public static String encodeStringArray(List<?> strings) {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(strings.size()).append("\r\n");
        for (Object str : strings) {
            sb.append(encodeBulkString(str.toString()));
        }
        return sb.toString();
    }

    public static String encodeError(String message) {
        return "-" + message + "\r\n";
    }

    public static String encodeMap(Map<?, ?> commands) {
        StringBuilder sb = new StringBuilder();
        sb.append("%").append(commands.size()).append("\r\n");
        for (Map.Entry<?, ?> entry : commands.entrySet()) {
            Object key = entry.getKey();
            switch (key) {
                case String s -> sb.append(encodeSimpleString(s));
                case Long l -> sb.append(encodeInteger(l));
                case Integer i -> sb.append(encodeInteger(i));
                case null, default ->
                        throw new IllegalArgumentException("Unsupported key type: " + key.getClass().getName());
            }
            Object value = entry.getValue();
            switch (value) {
                case String s -> sb.append(encodeBulkString(s));
                case Long l -> sb.append(encodeInteger(l));
                case Integer i -> sb.append(encodeInteger(i));
                case Map<?, ?> m -> sb.append(encodeMap(m));
                case List<?> l -> sb.append(encodeStringArray(l));
                case null, default ->
                        throw new IllegalArgumentException("Unsupported value type: " + value.getClass().getName());
            }
        }
        return sb.toString();
    }
}
