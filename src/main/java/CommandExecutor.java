import java.util.List;

public class CommandExecutor {

    private final Cache cache;

    public CommandExecutor(Cache cache) {
        this.cache = cache;
    }

    public String executeCommand(String command, List<String> args) {
        if ("ping".equalsIgnoreCase(command)) {
            return handlePing();
        } else if ("command".equalsIgnoreCase(command)) {
            return handleCommandsRequest();
        } else if ("echo".equalsIgnoreCase(command)) {
            return handleEchoRequest(args);
        } else if ("get".equalsIgnoreCase(command)) {
            return handleGetRequest(args);
        } else if ("set".equalsIgnoreCase(command)) {
            return handleSetRequest(args);
        } else {
            LoggingService.logError("Unknown command: " + command + " with args: " + args);
            return "-ERR unknown command '" + command + "'\r\n";
        }
    }

    private String handleSetRequest(List<String> args) {
        if (args.size() < 2) {
            return RESPEncoder.encodeError("ERR (SET) requires two arguments");
        }
        String key = args.getFirst();
        String value = args.get(1);
        cache.put(key, value);
        return RESPEncoder.encodeSimpleString("OK");
    }

    private String handleGetRequest(List<String> args) {
        if (args.isEmpty()) {
            return RESPEncoder.encodeError("ERR (GET) requires an argument");
        }
        String key = args.getFirst();
        String value = cache.get(key);
        return RESPEncoder.encodeBulkString(value);
    }

    private String handleEchoRequest(List<String> args) {
        if (args.isEmpty()) {
            return RESPEncoder.encodeError("ERR (ECHO) requires an argument");
        }
        String arg = args.getFirst();
        LoggingService.logFine("Echoing: " + arg);
        return RESPEncoder.encodeBulkString(arg);
    }

    private String handleCommandsRequest() {
        List<String> commands = List.of("ping", "command", "echo");
        LoggingService.logFine("Sending command list COMMAND.");
        return RESPEncoder.encodeStringArray(commands);
    }

    private String handlePing() {
        LoggingService.logFine("Sent PONG for PING.");
        return RESPEncoder.encodeSimpleString("PONG");
    }
}
