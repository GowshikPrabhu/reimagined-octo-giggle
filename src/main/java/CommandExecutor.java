import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CommandExecutor {

    public interface CommandHandler {
        String handleCommand(List<String> args);
    }

    private final Cache cache;
    private final Map<String, CommandHandler> commandHandlers = new HashMap<>();

    public CommandExecutor(Cache cache) {
        this.cache = cache;

        commandHandlers.put("command", this::handleCommandsRequest);
        commandHandlers.put("ping", this::handlePing);
        commandHandlers.put("echo", this::handleEchoRequest);
        commandHandlers.put("set", this::handleSetRequest);
        commandHandlers.put("get", this::handleGetRequest);
    }

    public String executeCommand(String command, List<String> args) {
        command = command.toLowerCase();
        CommandHandler handler = commandHandlers.get(command);

        if (handler != null) {
            return handler.handleCommand(args);
        } else {
            LoggingService.logError("Unknown command: " + command + " with args: " + args);
            return RESPEncoder.encodeError("ERR unknown command '" + command + "'");
        }
    }

    private String handleCommandsRequest(List<String> args) {
        List<String> commands = List.of("command", "ping", "echo", "set", "get");
        LoggingService.logFine("Sending command list COMMAND.");
        return RESPEncoder.encodeStringArray(commands);
    }

    private String handlePing(List<String> args) {
        String response = args.isEmpty() ? "PONG" : args.getFirst();
        LoggingService.logFine("Responding to PING with: " + response);
        return RESPEncoder.encodeSimpleString(response);
    }

    private String handleEchoRequest(List<String> args) {
        if (args.isEmpty()) {
            return RESPEncoder.encodeError("ERR wrong number of arguments for 'echo' command");
        }
        String arg = args.getFirst();
        LoggingService.logFine("Echoing: " + arg);
        return RESPEncoder.encodeBulkString(arg);
    }

    private String handleSetRequest(List<String> args) {
        if (args.size() < 2) {
            return RESPEncoder.encodeError("ERR wrong number of arguments for 'set' command");
        }

        String key = args.getFirst();
        String value = args.get(1);
        long expiresMillis = 0;

        for (int i = 2; i < args.size(); i++) {
            String option = args.get(i).toLowerCase();
            if (option.equals("px")) {
                if (i + 1 >= args.size()) {
                    return RESPEncoder.encodeError("ERR syntax error: PX requires a millisecond timeout");
                }
                try {
                    expiresMillis = Long.parseLong(args.get(i + 1));
                    if (expiresMillis <= 0) {
                        return RESPEncoder.encodeError("ERR PX milliseconds must be positive");
                    }
                } catch (NumberFormatException e) {
                    return RESPEncoder.encodeError("ERR PX argument must be a number");
                }
                i++;
            } else {
                return RESPEncoder.encodeError("ERR unknown option '" + option + "' for 'set' command");
            }
        }

        cache.put(key, value, expiresMillis);
        LoggingService.logFine("Set key '" + key + "' with TTL: " + expiresMillis + "ms");
        return RESPEncoder.encodeSimpleString("OK");
    }

    private String handleGetRequest(List<String> args) {
        if (args.isEmpty()) {
            return RESPEncoder.encodeError("ERR wrong number of arguments for 'get' command");
        }
        String key = args.getFirst();
        String value = cache.get(key);
        return RESPEncoder.encodeBulkString(value);
    }
}
