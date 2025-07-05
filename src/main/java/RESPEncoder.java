import java.util.List;

public class RESPEncoder {

    public static String encodeSimpleString(String s) {
        return "+" + s + "\r\n";
    }

    public static String encodeInteger(long num) {
        return ":" + num + "\r\n";
    }

    public static String encodeBulkString(String s) {
        return "$" + s.length() + "\r\n" + s + "\r\n";
    }

    public static String encodeStringArray(List<String> commands) {
        StringBuilder sb = new StringBuilder();
        sb.append("*").append(commands.size()).append("\r\n");
        for (String command : commands) {
            sb.append(encodeBulkString(command));
        }
        return sb.toString();
    }

    public static String encodeError(String s) {
        return "-" + s + "\r\n";
    }
}
