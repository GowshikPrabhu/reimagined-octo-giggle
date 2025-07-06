import java.util.*;
import java.util.regex.Pattern;

public class CommandExecutor {

    public interface CommandHandler {
        String handleCommand(List<String> args);
    }

    private final Cache cache;
    private final Map<String, CommandHandler> commandHandlers = new HashMap<>();

    public CommandExecutor() {
        this.cache = Cache.getInstance();

        commandHandlers.put("command", this::handleCommandsRequest);
        commandHandlers.put("ping", this::handlePing);
        commandHandlers.put("echo", this::handleEchoRequest);
        commandHandlers.put("set", this::handleSetRequest);
        commandHandlers.put("get", this::handleGetRequest);
        commandHandlers.put("config", this::handleConfigRequest);
        commandHandlers.put("keys", this::handleKeysRequest);
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
        List<String> commands = List.of("command", "ping", "echo", "set", "get", "config", "keys");
        if (args.isEmpty()) {
            LoggingService.logFine("Sending command list COMMAND.");
            return RESPEncoder.encodeStringArray(commands);
        } else {
            String arg = args.getFirst();
            if (arg.equalsIgnoreCase("docs")) {
                if (args.size() > 1) {
                    return RESPEncoder.encodeError("ERR Unimplemented subcommand 'docs' for 'command' command");
                }
                Map<String, Map<String, String>> commandDocs = new HashMap<>();
                for (String commandName : commands) {
                    Map<String, String> commandDoc = new HashMap<>();
                    commandDoc.put("summary", "Summary of " + commandName + " command");
                    commandDocs.put(commandName, commandDoc);
                }
                LoggingService.logFine("Sending command list DOCUMENTATION.");
                return RESPEncoder.encodeMap(commandDocs);

            }
            return RESPEncoder.encodeError("ERR unknown command '" + arg + "' for 'command' command");
        }
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

    public String handleConfigRequest(List<String> args) {
        if (args.size() < 2) {
            return RESPEncoder.encodeError("ERR wrong number of arguments for 'config' command");
        }
        String subCommand = args.getFirst().toLowerCase();
        if (subCommand.equals("get")) {
            String key = args.get(1);
            String value = Configs.getConfiguration(key);
            return value == null ? RESPEncoder.encodeStringArray(Collections.emptyList())
                    : RESPEncoder.encodeStringArray(List.of(key, value));
        } else if (subCommand.equals("set")) {
            if (args.size() < 3) {
                return RESPEncoder.encodeError("ERR wrong number of arguments for 'config set' command");
            }
            String key = args.get(1);
            String value = args.get(2);
            Configs.setConfiguration(key, value);
            return RESPEncoder.encodeSimpleString("OK");
        } else {
            return RESPEncoder.encodeError("ERR unknown sub command '" + subCommand + "' for 'config' command");
        }
    }

    public String handleKeysRequest(List<String> args) {
        if (args.isEmpty()) {
            return RESPEncoder.encodeError("ERR wrong number of arguments for 'keys' command");
        }
        String arg = args.getFirst();
        List<String> resultKeys;
        if (arg.equalsIgnoreCase("*")) {
            resultKeys = Arrays.asList(cache.keys());
        } else {
            String regexPattern = Globs.toRegexPattern(arg);
            Pattern pattern = Pattern.compile(regexPattern);
            resultKeys = Arrays.stream(cache.keys()).filter(key -> pattern.matcher(key).matches()).toList();
        }
        LoggingService.logFine("Sending keys list for prefix '" + arg + "': " + resultKeys);
        return RESPEncoder.encodeStringArray(resultKeys);
    }
}
