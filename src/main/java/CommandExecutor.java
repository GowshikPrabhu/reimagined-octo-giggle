import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.HexFormat;
import java.nio.channels.SocketChannel; // New import

public class CommandExecutor {

    public interface CommandHandler {
        void handleCommand(SocketChannel clientChannel, List<String> args, Consumer<String> stringWriter, Consumer<byte[]> byteWriter);
    }

    public interface ReplicationNotifier {
        void replicateCommand(List<String> commandParts);
        void registerSlaveChannel(SocketChannel slaveChannel);
        void removeConnectedSlave(SocketChannel slaveChannel);
        int getReplicationOffset();
    }

    private final Cache cache;
    private final Map<String, CommandHandler> commandHandlers = new HashMap<>();
    private ReplicationNotifier replicationNotifier;

    public CommandExecutor() {
        this.cache = Cache.getInstance();

        commandHandlers.put("command", this::handleCommandsRequest);
        commandHandlers.put("ping", this::handlePing);
        commandHandlers.put("echo", this::handleEchoRequest);
        commandHandlers.put("set", this::handleSetRequest);
        commandHandlers.put("get", this::handleGetRequest);
        commandHandlers.put("config", this::handleConfigRequest);
        commandHandlers.put("keys", this::handleKeysRequest);
        commandHandlers.put("info", this::handleInfoRequest);
        commandHandlers.put("replconf", this::handleReplConfRequest);
        commandHandlers.put("psync", this::handlePSyncRequest);
    }

    public void setReplicationNotifier(ReplicationNotifier notifier) {
        this.replicationNotifier = notifier;
    }

    public void executeCommand(SocketChannel clientChannel, String command, List<String> args, Consumer<String> stringWriter, Consumer<byte[]> byteWriter) {
        command = command.toLowerCase();
        CommandHandler handler = commandHandlers.get(command);

        if ("master".equalsIgnoreCase(Configs.getReplicationInfo("role"))) {
            if (command.equals("set") && replicationNotifier != null) {
                List<String> fullCommand = new ArrayList<>();
                fullCommand.add(command);
                fullCommand.addAll(args);
                replicationNotifier.replicateCommand(fullCommand);
                LoggingService.logFine("Replicated SET command to connected replicas: " + fullCommand);
            }
        }

        if (handler != null) {
            handler.handleCommand(clientChannel, args, stringWriter, byteWriter);
        } else {
            LoggingService.logError("Unknown command: " + command + " with args: " + args);
            stringWriter.accept(RESPEncoder.encodeError("ERR unknown command '" + command + "'"));
        }
    }

    private void handleCommandsRequest(SocketChannel clientChannel, List<String> args, Consumer<String> stringWriter, Consumer<byte[]> byteWriter) {
        List<String> commands = List.of("command", "ping", "echo", "set", "get", "config", "keys", "info", "replconf", "psync");
        if (args.isEmpty()) {
            LoggingService.logFine("Sending command list COMMAND.");
            stringWriter.accept(RESPEncoder.encodeStringArray(commands));
        } else {
            String arg = args.getFirst();
            if (arg.equalsIgnoreCase("docs")) {
                if (args.size() > 1) {
                    stringWriter.accept(RESPEncoder.encodeError("ERR Unimplemented subcommand 'docs' for 'command' command"));
                    return;
                }
                Map<String, Map<String, String>> commandDocs = new HashMap<>();
                for (String commandName : commands) {
                    Map<String, String> commandDoc = new HashMap<>();
                    commandDoc.put("summary", "Summary of " + commandName + " command");
                    commandDocs.put(commandName, commandDoc);
                }
                LoggingService.logFine("Sending command list DOCUMENTATION.");
                stringWriter.accept(RESPEncoder.encodeMap(commandDocs));
            } else {
                stringWriter.accept(RESPEncoder.encodeError("ERR unknown command '" + arg + "' for 'command' command"));
            }
        }
    }

    private void handlePing(SocketChannel clientChannel, List<String> args, Consumer<String> stringWriter, Consumer<byte[]> byteWriter) {
        String response = args.isEmpty() ? "PONG" : args.getFirst();
        LoggingService.logFine("Responding to PING with: " + response);
        stringWriter.accept(RESPEncoder.encodeSimpleString(response));
    }

    private void handleEchoRequest(SocketChannel clientChannel, List<String> args, Consumer<String> stringWriter, Consumer<byte[]> byteWriter) {
        if (args.isEmpty()) {
            stringWriter.accept(RESPEncoder.encodeError("ERR wrong number of arguments for 'echo' command"));
            return;
        }
        String arg = args.getFirst();
        LoggingService.logFine("Echoing: " + arg);
        stringWriter.accept(RESPEncoder.encodeBulkString(arg));
    }

    private void handleSetRequest(SocketChannel clientChannel, List<String> args, Consumer<String> stringWriter, Consumer<byte[]> byteWriter) {
        if (args.size() < 2) {
            stringWriter.accept(RESPEncoder.encodeError("ERR wrong number of arguments for 'set' command"));
            return;
        }

        String key = args.getFirst();
        String value = args.get(1);
        long expiresMillis = 0;

        for (int i = 2; i < args.size(); i++) {
            String option = args.get(i).toLowerCase();
            if (option.equals("px")) {
                if (i + 1 >= args.size()) {
                    stringWriter.accept(RESPEncoder.encodeError("ERR syntax error: PX requires a millisecond timeout"));
                    return;
                }
                try {
                    expiresMillis = Long.parseLong(args.get(i + 1));
                    if (expiresMillis <= 0) {
                        stringWriter.accept(RESPEncoder.encodeError("ERR PX milliseconds must be positive"));
                        return;
                    }
                } catch (NumberFormatException e) {
                    stringWriter.accept(RESPEncoder.encodeError("ERR PX argument must be a number"));
                    return;
                }
                i++;
            } else {
                stringWriter.accept(RESPEncoder.encodeError("ERR unknown option '" + option + "' for 'set' command"));
                return;
            }
        }

        cache.put(key, value, expiresMillis);
        LoggingService.logFine("Set key '" + key + "' with TTL: " + expiresMillis + "ms");
        stringWriter.accept(RESPEncoder.encodeSimpleString("OK"));
    }

    private void handleGetRequest(SocketChannel clientChannel, List<String> args, Consumer<String> stringWriter, Consumer<byte[]> byteWriter) {
        if (args.isEmpty()) {
            stringWriter.accept(RESPEncoder.encodeError("ERR wrong number of arguments for 'get' command"));
            return;
        }
        String key = args.getFirst();
        String value = cache.get(key);
        stringWriter.accept(RESPEncoder.encodeBulkString(value));
    }

    private void handleConfigRequest(SocketChannel clientChannel, List<String> args, Consumer<String> stringWriter, Consumer<byte[]> byteWriter) {
        if (args.size() < 2) {
            stringWriter.accept(RESPEncoder.encodeError("ERR wrong number of arguments for 'config' command"));
            return;
        }
        String subCommand = args.getFirst().toLowerCase();
        if (subCommand.equals("get")) {
            String key = args.get(1);
            String value = Configs.getConfiguration(key);
            stringWriter.accept(value == null ? RESPEncoder.encodeStringArray(Collections.emptyList()) : RESPEncoder.encodeStringArray(List.of(key, value)));
        } else if (subCommand.equals("set")) {
            if (args.size() < 3) {
                stringWriter.accept(RESPEncoder.encodeError("ERR wrong number of arguments for 'config set' command"));
                return;
            }
            String key = args.get(1);
            String value = args.get(2);
            Configs.setConfiguration(key, value);
            stringWriter.accept(RESPEncoder.encodeSimpleString("OK"));
        } else {
            stringWriter.accept(RESPEncoder.encodeError("ERR unknown sub command '" + subCommand + "' for 'config' command"));
        }
    }

    private void handleKeysRequest(SocketChannel clientChannel, List<String> args, Consumer<String> stringWriter, Consumer<byte[]> byteWriter) {
        if (args.isEmpty()) {
            stringWriter.accept(RESPEncoder.encodeError("ERR wrong number of arguments for 'keys' command"));
            return;
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
        stringWriter.accept(RESPEncoder.encodeStringArray(resultKeys));
    }

    private void handleInfoRequest(SocketChannel clientChannel, List<String> args, Consumer<String> stringWriter, Consumer<byte[]> byteWriter) {
        if (args.isEmpty()) {
            stringWriter.accept(RESPEncoder.encodeError("ERR empty info command unimplemented"));
            return;
        }
        String arg = args.getFirst();
        if (arg.equalsIgnoreCase("replication")) {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, String> entry : Configs.getReplicationInfo().entrySet()) {
                sb.append(entry.getKey()).append(":").append(entry.getValue()).append("\n");
            }
            stringWriter.accept(RESPEncoder.encodeBulkString(sb.toString()));
        } else {
            stringWriter.accept(RESPEncoder.encodeError("ERR unknown info subcommand '" + arg + "'"));
        }
    }

    private void handleReplConfRequest(SocketChannel clientChannel, List<String> args, Consumer<String> stringWriter, Consumer<byte[]> byteWriter) {
        if (args.isEmpty()) {
            stringWriter.accept(RESPEncoder.encodeError("ERR empty replconf command unimplemented"));
            return;
        }
        String arg = args.getFirst();
        if (arg.equalsIgnoreCase("listening-port")) {
            if (args.size() < 2) {
                stringWriter.accept(RESPEncoder.encodeError("ERR wrong number of arguments for 'replconf listening-port' command"));
                return;
            }
            arg = args.get(1);
            LoggingService.logInfo("Got REPLCONF with listening-port: " + arg);
            if ("master".equalsIgnoreCase(Configs.getReplicationInfo("role")) && replicationNotifier != null) {
                replicationNotifier.registerSlaveChannel(clientChannel);
            }
            stringWriter.accept(RESPEncoder.encodeSimpleString("OK"));
        } else if (arg.equalsIgnoreCase("capa")) {
            if (args.size() < 2) {
                stringWriter.accept(RESPEncoder.encodeError("ERR wrong number of arguments for 'replconf capa' command"));
                return;
            }
            arg = args.get(1);
            LoggingService.logInfo("Got REPLCONF with capa: " + arg);
            stringWriter.accept(RESPEncoder.encodeSimpleString("OK"));
        } else if (arg.equalsIgnoreCase("GETACK")) {
            if (args.size() < 2) {
                stringWriter.accept(RESPEncoder.encodeError("ERR wrong number of arguments for 'replconf GETACK' command"));
                return;
            }
            arg = args.get(1);
            LoggingService.logInfo("Got REPLCONF with GETACK: " + arg);
            int replicationOffset = replicationNotifier.getReplicationOffset();
            String s = RESPEncoder.encodeStringArray(List.of("REPLCONF", "ACK", replicationOffset));
            LoggingService.logInfo("Sending REPLCONF ACK: " + s);
            stringWriter.accept(s);
        } else {
            stringWriter.accept(RESPEncoder.encodeError("ERR unknown replconf subcommand '" + arg + "'"));
        }
    }

    private void handlePSyncRequest(SocketChannel clientChannel, List<String> args, Consumer<String> stringWriter, Consumer<byte[]> byteWriter) {
        if (args.isEmpty() || args.size() < 2) {
            stringWriter.accept(RESPEncoder.encodeError("ERR wrong number of arguments for 'psync' command"));
            return;
        }
        String replicationID = args.getFirst();
        String offset = args.get(1);
        LoggingService.logInfo("Got PSYNC with replicationID: " + replicationID + " and offset: " + offset);
        stringWriter.accept(RESPEncoder.encodeSimpleString("FULLRESYNC " + Configs.getReplicationInfo("master_replid") + " 0"));
        String dummyHex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
        byte[] bytes = HexFormat.of().parseHex(dummyHex);
        byte[] rdb = RESPEncoder.encodeBinary(bytes);
        LoggingService.logInfo("Sending dummy RDB. Size: " + rdb.length);
        byteWriter.accept(rdb);
    }
}
