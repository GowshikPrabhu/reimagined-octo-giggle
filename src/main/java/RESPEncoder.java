import java.util.List;

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

    public static String encodeStringArray(List<String> strings) {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(strings.size()).append("\r\n");
        for (String str : strings) {
            sb.append(encodeBulkString(str));
        }
        return sb.toString();
    }

    public static String encodeError(String message) {
        return "-" + message + "\r\n";
    }
}
