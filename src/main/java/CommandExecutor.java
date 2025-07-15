import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.nio.channels.SocketChannel; // New import

public class CommandExecutor {

    public interface CommandHandler {
        void handleCommand(SocketChannel clientChannel, List<String> args, Consumer<String> stringWriter, Consumer<byte[]> byteWriter, int bytesConsumed);
    }

    public interface ReplicationNotifier {
        void replicateCommand(List<String> commandParts);
        void registerSlaveChannel(SocketChannel slaveChannel);
        void removeConnectedSlave(SocketChannel slaveChannel);
        long getReplicationOffset();
        Map<SocketChannel, Long> getSlaveAckOffsets();
        Set<SocketChannel> getConnectedSlaves();
    }

    private final Cache cache;
    private final Map<String, CommandHandler> commandHandlers = new HashMap<>();
    private ReplicationNotifier replicationNotifier;

    private final ConcurrentLinkedQueue<PendingWaitRequest> pendingWaitRequests = new ConcurrentLinkedQueue<>();

    public final ConcurrentMap<String, List<BlockedClient>> blockedClientsPerStream = new ConcurrentHashMap<>();

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
        commandHandlers.put("wait", this::handleWaitRequest);
        commandHandlers.put("type", this::handleTypeRequest);
        commandHandlers.put("xadd", this::handleXaddRequest);
        commandHandlers.put("xrange", this::handleXRangeRequest);
        commandHandlers.put("xread", this::handleXReadRequest);
    }

    public void setReplicationNotifier(ReplicationNotifier notifier) {
        this.replicationNotifier = notifier;
    }

    public void executeCommand(SocketChannel clientChannel, String command, List<String> args, Consumer<String> stringWriter, Consumer<byte[]> byteWriter, int bytesConsumed) {
        command = command.toLowerCase();
        CommandHandler handler = commandHandlers.get(command);

        if ("master".equalsIgnoreCase(Configs.getReplicationInfoAsString("role"))) {
            if (command.equals("set") && replicationNotifier != null) {
                List<String> fullCommand = new ArrayList<>();
                fullCommand.add(command);
                fullCommand.addAll(args);
                replicationNotifier.replicateCommand(fullCommand);
                LoggingService.logFine("Replicated SET command to connected replicas: " + fullCommand);
            }
        }

        if (handler != null) {
            handler.handleCommand(clientChannel, args, stringWriter, byteWriter, bytesConsumed);
        } else {
            LoggingService.logError("Unknown command: " + command + " with args: " + args);
            stringWriter.accept(RESPEncoder.encodeError("ERR unknown command '" + command + "'"));
        }
    }

    public void processPendingWaitRequests() {
        if (replicationNotifier == null) {
            LoggingService.logWarn("Cannot process pending WAIT requests: ReplicationNotifier not set.");
            return;
        }

        Iterator<PendingWaitRequest> iterator = pendingWaitRequests.iterator();

        while (iterator.hasNext()) {
            PendingWaitRequest pending = iterator.next();

            long acknowledgedSlaves = replicationNotifier.getSlaveAckOffsets().values().stream()
                .filter(offset -> offset >= pending.masterOffset)
                .count();

            if (acknowledgedSlaves >= pending.requiredSlaves) {
                LoggingService.logInfo("Master: WAIT condition met for client " + pending.clientChannel + ". Slaves acknowledged: " + acknowledgedSlaves);
                try {
                    pending.stringWriter.accept(RESPEncoder.encodeInteger(acknowledgedSlaves));
                } catch (Exception e) {
                    LoggingService.logError("Error writing WAIT response to client " + pending.clientChannel + ": " + e.getMessage(), e);
                }
                pending.latch.countDown();
                iterator.remove();
            } else if (pending.timeoutMillis > 0 && (System.currentTimeMillis() - pending.startTime >= pending.timeoutMillis)) {
                LoggingService.logInfo("Master: Pending WAIT command timed out for client " + pending.clientChannel + ". Slaves acknowledged: " + acknowledgedSlaves);
                try {
                    pending.stringWriter.accept(RESPEncoder.encodeInteger(acknowledgedSlaves));
                } catch (Exception e) {
                    LoggingService.logError("Error writing WAIT timeout response to client " + pending.clientChannel + ": " + e.getMessage(), e);
                }
                pending.latch.countDown();
                iterator.remove();
            }
        }
    }

    private void handleCommandsRequest(SocketChannel clientChannel, List<String> args, Consumer<String> stringWriter, Consumer<byte[]> byteWriter, int bytesConsumed) {
        List<String> commands = commandHandlers.keySet().stream().toList();
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
                List<Object> commandDocs = new ArrayList<>();
                for (String commandName : commands) {
                    List<String> docs = new ArrayList<>();
                    docs.add("summary");
                    docs.add("Summary of " + commandName + " command");
                    docs.add("since");
                    docs.add("1.0.0");
                    commandDocs.add(commandName);
                    commandDocs.add(docs);
                }
                LoggingService.logInfo("Sending command list DOCUMENTATION.");
                stringWriter.accept(RESPEncoder.encodeArray(commandDocs));
            } else {
                stringWriter.accept(RESPEncoder.encodeError("ERR unknown command '" + arg + "' for 'command' command"));
            }
        }
    }

    private void handlePing(SocketChannel clientChannel, List<String> args, Consumer<String> stringWriter, Consumer<byte[]> byteWriter, int bytesConsumed) {
        String response = args.isEmpty() ? "PONG" : args.getFirst();
        LoggingService.logFine("Responding to PING with: " + response);
        stringWriter.accept(RESPEncoder.encodeSimpleString(response));
    }

    private void handleEchoRequest(SocketChannel clientChannel, List<String> args, Consumer<String> stringWriter, Consumer<byte[]> byteWriter, int bytesConsumed) {
        if (args.isEmpty()) {
            stringWriter.accept(RESPEncoder.encodeError("ERR wrong number of arguments for 'echo' command"));
            return;
        }
        String arg = args.getFirst();
        LoggingService.logFine("Echoing: " + arg);
        stringWriter.accept(RESPEncoder.encodeBulkString(arg));
    }

    private void handleSetRequest(SocketChannel clientChannel, List<String> args, Consumer<String> stringWriter, Consumer<byte[]> byteWriter, int bytesConsumed) {
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

        cache.put(key, new Cache.Value(value, Cache.TYPE_STRING), expiresMillis);
        LoggingService.logFine("Set key '" + key + "' with TTL: " + expiresMillis + "ms");
        stringWriter.accept(RESPEncoder.encodeSimpleString("OK"));
        if ("master".equalsIgnoreCase(Configs.getReplicationInfoAsString("role"))) {
            long offset = (long) Configs.getReplicationInfo("master_repl_offset");
            Configs.setReplicationInfo("master_repl_offset", offset + bytesConsumed);
        }
    }

    private void handleGetRequest(SocketChannel clientChannel, List<String> args, Consumer<String> stringWriter, Consumer<byte[]> byteWriter, int bytesConsumed) {
        if (args.isEmpty()) {
            stringWriter.accept(RESPEncoder.encodeError("ERR wrong number of arguments for 'get' command"));
            return;
        }
        String key = args.getFirst();
        Cache.Value value = cache.get(key);
        String strValue = value != null ? value.getValue().toString() : null;
        stringWriter.accept(RESPEncoder.encodeBulkString(strValue));
    }

    private void handleConfigRequest(SocketChannel clientChannel, List<String> args, Consumer<String> stringWriter, Consumer<byte[]> byteWriter, int bytesConsumed) {
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

    private void handleKeysRequest(SocketChannel clientChannel, List<String> args, Consumer<String> stringWriter, Consumer<byte[]> byteWriter, int bytesConsumed) {
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

    private void handleInfoRequest(SocketChannel clientChannel, List<String> args, Consumer<String> stringWriter, Consumer<byte[]> byteWriter, int bytesConsumed) {
        if (args.isEmpty()) {
            stringWriter.accept(RESPEncoder.encodeError("ERR empty info command unimplemented"));
            return;
        }
        String arg = args.getFirst();
        if (arg.equalsIgnoreCase("replication")) {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, Object> entry : Configs.getReplicationInfo().entrySet()) {
                sb.append(entry.getKey()).append(":").append(entry.getValue().toString()).append("\n");
            }
            stringWriter.accept(RESPEncoder.encodeBulkString(sb.toString()));
        } else {
            stringWriter.accept(RESPEncoder.encodeError("ERR unknown info subcommand '" + arg + "'"));
        }
    }

    private void handleReplConfRequest(SocketChannel clientChannel, List<String> args, Consumer<String> stringWriter, Consumer<byte[]> byteWriter, int bytesConsumed) {
        if (args.isEmpty()) {
            stringWriter.accept(RESPEncoder.encodeError("ERR empty replconf command unimplemented"));
            return;
        }
        String subCommand = args.getFirst().toLowerCase();
        switch (subCommand) {
            case "listening-port":
                if (args.size() < 2) {
                    stringWriter.accept(RESPEncoder.encodeError("ERR wrong number of arguments for 'replconf listening-port' command"));
                    return;
                }
                LoggingService.logInfo("Got REPLCONF with listening-port: " + args.get(1));
                if ("master".equalsIgnoreCase(Configs.getReplicationInfoAsString("role")) && replicationNotifier != null) {
                    replicationNotifier.registerSlaveChannel(clientChannel);
                }
                stringWriter.accept(RESPEncoder.encodeSimpleString("OK"));
                break;
            case "capa":
                if (args.size() < 2) {
                    stringWriter.accept(RESPEncoder.encodeError("ERR wrong number of arguments for 'replconf capa' command"));
                    return;
                }
                LoggingService.logInfo("Got REPLCONF with capa: " + args.get(1));
                stringWriter.accept(RESPEncoder.encodeSimpleString("OK"));
                break;
            case "getack":
                if (args.isEmpty()) {
                    stringWriter.accept(RESPEncoder.encodeError("ERR wrong number of arguments for 'replconf GETACK' command"));
                    return;
                }
                if ("master".equalsIgnoreCase(Configs.getReplicationInfoAsString("role"))) {
                     LoggingService.logWarn("Master: Received REPLCONF GETACK from client " + clientChannel + ". This should not happen directly from a general client. Only internal WAIT uses it.");
                     stringWriter.accept(RESPEncoder.encodeError("ERR REPLCONF GETACK only for master-slave communication"));
                } else {
                    if (replicationNotifier == null) {
                         stringWriter.accept(RESPEncoder.encodeError("ERR ReplicationNotifier not initialized for ACK"));
                         return;
                    }
                    long replicationOffset = replicationNotifier.getReplicationOffset();
                    String s = RESPEncoder.encodeStringArray(List.of("REPLCONF", "ACK", String.valueOf(replicationOffset)));
                    LoggingService.logInfo("Slave: Sending REPLCONF ACK: " + s + " with offset: " + replicationOffset);
                    stringWriter.accept(s);
                }
                break;
            case "ack":
                if ("master".equalsIgnoreCase(Configs.getReplicationInfoAsString("role")) && args.size() == 2) {
                    try {
                        long slaveOffset = Long.parseLong(args.get(1));
                        if (replicationNotifier != null) {
                            replicationNotifier.getSlaveAckOffsets().put(clientChannel, slaveOffset);
                        }
                        LoggingService.logInfo("Master: Received REPLCONF ACK from slave " + clientChannel + " with offset: " + slaveOffset);
                        processPendingWaitRequests();
                    } catch (NumberFormatException e) {
                        LoggingService.logError("Master: Invalid ACK offset from slave " + clientChannel + ": " + args.get(1), e);
                        stringWriter.accept(RESPEncoder.encodeError("ERR invalid ACK offset"));
                    }
                } else {
                    stringWriter.accept(RESPEncoder.encodeError("ERR invalid replconf ack command"));
                }
                break;
            default:
                stringWriter.accept(RESPEncoder.encodeError("ERR unknown replconf subcommand '" + subCommand + "'"));
                break;
        }
    }

    private void handlePSyncRequest(SocketChannel clientChannel, List<String> args, Consumer<String> stringWriter, Consumer<byte[]> byteWriter, int bytesConsumed) {
        if (args.isEmpty() || args.size() < 2) {
            stringWriter.accept(RESPEncoder.encodeError("ERR wrong number of arguments for 'psync' command"));
            return;
        }
        String replicationID = args.getFirst();
        String offset = args.get(1);
        LoggingService.logInfo("Got PSYNC with replicationID: " + replicationID + " and offset: " + offset);
        stringWriter.accept(RESPEncoder.encodeSimpleString("FULLRESYNC " + Configs.getReplicationInfoAsString("master_replid") + " 0"));
        String dummyHex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
        byte[] bytes = HexFormat.of().parseHex(dummyHex);
        byte[] rdb = RESPEncoder.encodeBinary(bytes);
        LoggingService.logInfo("Sending dummy RDB. Size: " + rdb.length);
        byteWriter.accept(rdb);
    }

    private void handleWaitRequest(SocketChannel clientChannel, List<String> args, Consumer<String> stringWriter, Consumer<byte[]> byteWriter, int bytesConsumed) {
        LoggingService.logInfo("Start of wait: " + System.currentTimeMillis());
        if ("slave".equalsIgnoreCase(Configs.getReplicationInfoAsString("role"))) {
            stringWriter.accept(RESPEncoder.encodeError("ERR WAIT command is only available when the server is a master."));
            return;
        }
        if (args.size() < 2) {
            stringWriter.accept(RESPEncoder.encodeError("ERR wrong number of arguments for 'wait' command"));
            return;
        }
        if (replicationNotifier == null) {
            stringWriter.accept(RESPEncoder.encodeError("ERR ReplicationNotifier not initialized. Cannot handle WAIT."));
            return;
        }

        try {
            int requiredSlaves = Integer.parseInt(args.get(0));
            long timeoutMillis = Long.parseLong(args.get(1));
            LoggingService.logInfo("Master: Received WAIT command. Required Slaves: " + requiredSlaves + ", Timeout: " + timeoutMillis + "ms");

            long currentMasterOffset = replicationNotifier.getReplicationOffset();
            LoggingService.logInfo("Master: Current replication offset: " + currentMasterOffset);

            if (replicationNotifier.getConnectedSlaves().isEmpty()) {
                LoggingService.logInfo("Master: No slaves connected. Responding with 0 for WAIT command.");
                stringWriter.accept(RESPEncoder.encodeInteger(0));
                return;
            }

            long acknowledgedSlavesCount = replicationNotifier.getSlaveAckOffsets().values().stream()
                .filter(offset -> offset >= currentMasterOffset)
                .count();

            if (acknowledgedSlavesCount >= requiredSlaves) {
                LoggingService.logInfo("Master: WAIT condition met immediately. Slaves acknowledged: " + acknowledgedSlavesCount);
                try {
                    LoggingService.logInfo("Writing to write to channel: " + clientChannel.getRemoteAddress());
                } catch (IOException e) {
                    LoggingService.logError("Master: Error getting remote address for client channel: " + e.getMessage(), e);
                }
                stringWriter.accept(RESPEncoder.encodeInteger(acknowledgedSlavesCount));
                return;
            }

            CountDownLatch latch = new CountDownLatch(1);
            PendingWaitRequest pending = new PendingWaitRequest(clientChannel, stringWriter, requiredSlaves, currentMasterOffset, timeoutMillis, latch);
            pendingWaitRequests.offer(pending);

            replicationNotifier.replicateCommand(List.of("REPLCONF", "GETACK", "*"));
            LoggingService.logInfo("Master: Sent REPLCONF GETACK * to all slaves for WAIT command.");

            Thread waitThread = new Thread(() -> {
                try {
                    boolean completed = latch.await(timeoutMillis, TimeUnit.MILLISECONDS);
                    if (!completed) {
                        LoggingService.logInfo("Master: WAIT command timed out after " + timeoutMillis + "ms for client " + clientChannel);
                        processPendingWaitRequests();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LoggingService.logError("Master: WAIT command interrupted for client " + clientChannel + ": " + e.getMessage(), e);
                    stringWriter.accept(RESPEncoder.encodeError("ERR WAIT command interrupted"));
                    pendingWaitRequests.remove(pending);
                }
            });
            waitThread.setName("RedisWaitClient-" + clientChannel.socket().getPort());
            waitThread.start();

        } catch (NumberFormatException e) {
            stringWriter.accept(RESPEncoder.encodeError("ERR invalid number format in 'wait' command"));
        }
    }

    private void handleTypeRequest(SocketChannel clientChannel, List<String> args, Consumer<String> stringWriter, Consumer<byte[]> byteWriter, int bytesConsumed) {
        if (args.isEmpty()) {
            stringWriter.accept(RESPEncoder.encodeError("ERR wrong number of arguments for 'type' command"));
            return;
        }
        String key = args.getFirst();
        Cache.Value value = cache.get(key);
        if (value == null) {
            stringWriter.accept(RESPEncoder.encodeSimpleString("none"));
        } else {
            stringWriter.accept(RESPEncoder.encodeSimpleString(value.getType()));
        }
    }

    private void handleXaddRequest(SocketChannel clientChannel, List<String> args, Consumer<String> stringWriter, Consumer<byte[]> byteWriter, int bytesConsumed) {
        if (args.size() < 4) {
            stringWriter.accept(RESPEncoder.encodeError("ERR wrong number of arguments for 'xadd' command"));
            return;
        }
        String streamKey = args.getFirst();
        String idArg = args.get(1);

        TreeMap<Long, NavigableMap<Long, Map<String, String>>> streamEntries;

        Cache.Value value = cache.get(streamKey);
        if (value == null) {
            streamEntries = new TreeMap<>();
        } else if (value.getType().equals(Cache.TYPE_STREAM)) {
            //noinspection unchecked
            streamEntries = (TreeMap<Long, NavigableMap<Long, Map<String, String>>>) value.getValue();
        } else {
            stringWriter.accept(RESPEncoder.encodeError("ERR value is not a stream"));
            return;
        }

        long milliseconds;
        long sequence;

        if (idArg.equals("*")) {
            milliseconds = System.currentTimeMillis();
            NavigableMap<Long, Map<String, String>> seqMap = streamEntries.computeIfAbsent(milliseconds, _ -> new TreeMap<>());
            sequence = seqMap.isEmpty() ? 0 : seqMap.lastKey() + 1;
        } else if (idArg.endsWith("-*")) {
            String msStr = idArg.substring(0, idArg.length() - 2);
            try {
                milliseconds = Long.parseLong(msStr);
            } catch (NumberFormatException e) {
                stringWriter.accept(RESPEncoder.encodeError("ERR invalid milliseconds in ID"));
                return;
            }
            NavigableMap<Long, Map<String, String>> seqMap = streamEntries.computeIfAbsent(milliseconds, _ -> new TreeMap<>());
            sequence = seqMap.isEmpty() ? (milliseconds == 0 ? 1 : 0) : seqMap.lastKey() + 1;
        } else {
            String[] parts = idArg.split("-");
            if (parts.length != 2) {
                stringWriter.accept(RESPEncoder.encodeError("ERR invalid ID format"));
                return;
            }
            try {
                milliseconds = Long.parseLong(parts[0]);
                sequence = Long.parseLong(parts[1]);
            } catch (NumberFormatException e) {
                stringWriter.accept(RESPEncoder.encodeError("ERR invalid ID format"));
                return;
            }
            if (milliseconds == 0 && sequence == 0) {
                stringWriter.accept(RESPEncoder.encodeError("ERR The ID specified in XADD must be greater than 0-0"));
                return;
            }
            if (!streamEntries.isEmpty()) {
                Long lastTime = streamEntries.lastKey();
                if (milliseconds < lastTime) {
                    stringWriter.accept(RESPEncoder.encodeError("ERR The ID specified in XADD is equal or smaller than the target stream top item"));
                    return;
                }
            }
            NavigableMap<Long, Map<String, String>> seqMap = streamEntries.computeIfAbsent(milliseconds, _ -> new TreeMap<>());
            if (seqMap.containsKey(sequence)) {
                stringWriter.accept(RESPEncoder.encodeError("ERR The ID specified in XADD is equal or smaller than the target stream top item"));
                return;
            } else {
                if (!seqMap.isEmpty()) {
                    Long lastSequence = seqMap.lastKey();
                    if (lastSequence != null && sequence <= lastSequence) {
                        stringWriter.accept(RESPEncoder.encodeError("ERR The ID specified in XADD is equal or smaller than the target stream top item"));
                        return;
                    }
                }
            }
        }

        Map<String, String> entryFields = new HashMap<>();
        for (int i = 2; i < args.size(); i += 2) {
            if (i + 1 >= args.size()) {
                stringWriter.accept(RESPEncoder.encodeError("ERR wrong number of arguments for 'xadd' command"));
                return;
            }

            entryFields.put(args.get(i), args.get(i + 1));
        }
        streamEntries.get(milliseconds).put(sequence, entryFields);
        cache.put(streamKey, new Cache.Value(streamEntries, Cache.TYPE_STREAM), 0);
        String id = milliseconds + "-" + sequence;
        if (idArg.equals("*")) {
            stringWriter.accept(RESPEncoder.encodeBulkString(id));
        } else {
            stringWriter.accept(RESPEncoder.encodeSimpleString(id));
        }

        List<BlockedClient> blockedClients = blockedClientsPerStream.get(streamKey);
        if (blockedClients != null) {
            Iterator<BlockedClient> iter = blockedClients.iterator();
            while (iter.hasNext()) {
                BlockedClient bc = iter.next();
                List<Object> result = fetchStreamEntries(bc.streamKeys(), bc.streamIds(), bc.count(), new ArrayList<>());
                if (!result.isEmpty()) {
                    bc.stringWriter().accept(RESPEncoder.encodeArray(result));
                    iter.remove();
                }
            }
            if (blockedClients.isEmpty()) {
                blockedClientsPerStream.remove(streamKey);
            }
        }

    }

    private void handleXRangeRequest(SocketChannel clientChannel, List<String> args, Consumer<String> stringWriter, Consumer<byte[]> byteWriter, int bytesConsumed) {
        if (args.size() < 2 || args.size() > 5) {
            stringWriter.accept(RESPEncoder.encodeError("ERR wrong number of arguments for 'xrange' command"));
            return;
        }
        String streamKey = args.getFirst();
        Cache.Value value = cache.get(streamKey);
        if (value == null || !Cache.TYPE_STREAM.equals(value.getType())) {
            stringWriter.accept(RESPEncoder.encodeError("ERR no such stream: " + streamKey));
            return;
        }

        @SuppressWarnings("unchecked")
        TreeMap<Long, NavigableMap<Long, Map<String, String>>> streamEntries = (TreeMap<Long, NavigableMap<Long, Map<String, String>>>) value.getValue();

        String startId = args.get(1);
        String endId = args.get(2);
        int count = args.size() > 3 ? Integer.parseInt(args.get(4)) : -1;

        long startMs, startSeq;
        long endMs, endSeq;

        try {
            if (startId.equals("-")) {
                startMs = 0;
                startSeq = 0;
            } else {
                String[] parts = startId.split("-");
                startMs = Long.parseLong(parts[0]);
                startSeq = parts.length > 1 ? Long.parseLong(parts[1]) : 0;
            }

            if (endId.equals("+")) {
                endMs = Long.MAX_VALUE;
                endSeq = Long.MAX_VALUE;
            } else {
                String[] parts = endId.split("-");
                endMs = Long.parseLong(parts[0]);
                endSeq = parts.length > 1 ? Long.parseLong(parts[1]) : Long.MAX_VALUE;
            }
        } catch (NumberFormatException e) {
            stringWriter.accept(RESPEncoder.encodeError("ERR invalid ID format in 'xrange' command"));
            return;
        }

        List<Object> resultEntries = new ArrayList<>();

        LoggingService.logInfo("Handling XRANGE for stream: %s, startId: %s, endId: %s, startMS: %s, endMS: %s, startSeq: %s, endSeq: %s, count: %d".formatted(streamKey, startId, endId, startMs, endMs, startSeq, endSeq, count));

        for (Map.Entry<Long, NavigableMap<Long, Map<String, String>>> entry : streamEntries.subMap(startMs, true, endMs, true).entrySet()) {
            long ms = entry.getKey();
            long expectedStartSeq = (ms == startMs) ? startSeq : 0;
            long expectedEndSeq = (ms == endMs) ? endSeq : Long.MAX_VALUE;
            for (Map.Entry<Long, Map<String, String>> seqEntry : entry.getValue().subMap(expectedStartSeq, true, expectedEndSeq, true).entrySet()) {
                Map<String, String> fields = seqEntry.getValue();
                List<Object> entryData = new ArrayList<>();
                entryData.add(ms + "-" + seqEntry.getKey());
                List<Object> fieldData = new ArrayList<>();
                fields.forEach((k, v) -> {
                    fieldData.add(k);
                    fieldData.add(v);
                });
                entryData.add(fieldData);
                resultEntries.add(entryData);
                if (count > 0 && resultEntries.size() == count) {
                    break;
                }
            }
            if (count > 0 && resultEntries.size() == count) {
                break;
            }
        }
        if (resultEntries.isEmpty()) {
            stringWriter.accept(RESPEncoder.encodeArray(Collections.emptyList()));
        } else {
            stringWriter.accept(RESPEncoder.encodeArray(resultEntries));
        }
    }

    private void handleXReadRequest(SocketChannel clientChannel, List<String> args,
                                    Consumer<String> stringWriter, Consumer<byte[]> byteWriter, int bytesConsumed) {
        int count = 100;
        long blockMillis = -1;
        int idx = 0;

        while (idx < args.size()) {
            String arg = args.get(idx).toUpperCase();
            if (arg.equals("COUNT") && idx + 1 < args.size()) {
                try {
                    count = Integer.parseInt(args.get(idx + 1));
                } catch (NumberFormatException e) {
                    stringWriter.accept(RESPEncoder.encodeError("ERR invalid COUNT value in 'xread' command"));
                    return;
                }
                idx += 2;
            } else if (arg.equals("BLOCK") && idx + 1 < args.size()) {
                try {
                    blockMillis = Long.parseLong(args.get(idx + 1));
                } catch (NumberFormatException e) {
                    stringWriter.accept(RESPEncoder.encodeError("ERR invalid BLOCK value in 'xread' command"));
                    return;
                }
                idx += 2;
            } else {
                break;
            }
        }

        if (idx >= args.size() || !args.get(idx).equalsIgnoreCase("STREAMS")) {
            stringWriter.accept(RESPEncoder.encodeError("ERR syntax error, missing STREAMS in 'xread' command"));
            return;
        }
        idx++;

        int streamsCount = (args.size() - idx) / 2;
        if (streamsCount <= 0 || idx + streamsCount * 2 > args.size()) {
            stringWriter.accept(RESPEncoder.encodeError("ERR wrong number of arguments for 'xread' command"));
            return;
        }
        List<String> keys = args.subList(idx, idx + streamsCount);
        List<String> ids = args.subList(idx + streamsCount, idx + streamsCount * 2);

        List<String> updatedIds = new ArrayList<>();
        List<Object> result = fetchStreamEntries(keys, ids, count, updatedIds);

        if (!result.isEmpty()) {
            stringWriter.accept(RESPEncoder.encodeArray(result));
            return;
        }

        if (blockMillis > -1) {
            long unblockAt = blockMillis == 0 ? Long.MAX_VALUE : System.currentTimeMillis() + blockMillis;
            BlockedClient blockedClient = new BlockedClient(clientChannel, keys, updatedIds, count, unblockAt, stringWriter, byteWriter);

            for (String key : keys) {
                blockedClientsPerStream.computeIfAbsent(key, _ -> new ArrayList<>()).add(blockedClient);
            }
            return;
        }

        stringWriter.accept(RESPEncoder.encodeNull());
    }


    private List<Object> fetchStreamEntries(List<String> keys, List<String> ids, int count, List<String> updatedIds) {
        List<Object> result = new ArrayList<>();

        for (int i = 0; i < keys.size(); i++) {
            String streamKey = keys.get(i);
            String id = ids.get(i);

            Cache.Value value = cache.get(streamKey);
            if (value == null || !Cache.TYPE_STREAM.equals(value.getType())) {
                continue;
            }

            @SuppressWarnings("unchecked")
            TreeMap<Long, NavigableMap<Long, Map<String, String>>> streamEntries =
                    (TreeMap<Long, NavigableMap<Long, Map<String, String>>>) value.getValue();

            long startMs, startSeq;
            if (id.equals("$")) {
                if (streamEntries.isEmpty()) {
                    startMs = 0;
                    startSeq = 0;
                } else {
                    Map.Entry<Long, NavigableMap<Long, Map<String, String>>> lastEntry = streamEntries.lastEntry();
                    startMs = lastEntry.getKey();
                    startSeq = lastEntry.getValue().lastKey();
                }
                id = startMs + "-" + startSeq;
                updatedIds.add(id);
            } else {
                try {
                    String[] parts = id.split("-");
                    startMs = Long.parseLong(parts[0]);
                    startSeq = parts.length > 1 ? Long.parseLong(parts[1]) : 0;
                } catch (NumberFormatException e) {
                    continue;
                }
                updatedIds.add(id);
            }

            List<Object> entries = new ArrayList<>();
            LoggingService.logInfo("Fetching entries for stream: %s, startId: %s, startMS: %s, startSeq: %s, count: %d".formatted(streamKey, id, startMs, startSeq, count));
            outer:
            for (Map.Entry<Long, NavigableMap<Long, Map<String, String>>> entry : streamEntries.tailMap(startMs, true).entrySet()) {
                long ms = entry.getKey();
                long expectedStartSeq = (ms == startMs) ? startSeq : -1;
                for (Map.Entry<Long, Map<String, String>> seqEntry : entry.getValue().tailMap(expectedStartSeq, false).entrySet()) {
                    Map<String, String> fields = seqEntry.getValue();
                    List<Object> entryData = new ArrayList<>();
                    entryData.add(ms + "-" + seqEntry.getKey());
                    List<Object> fieldData = new ArrayList<>();
                    fields.forEach((k, v) -> {
                        fieldData.add(k);
                        fieldData.add(v);
                    });
                    entryData.add(fieldData);
                    entries.add(entryData);
                    if (entries.size() == count) {
                        break outer;
                    }
                }
            }
            if (!entries.isEmpty()) {
                List<Object> streamData = new ArrayList<>();
                streamData.add(streamKey);
                streamData.add(entries);
                result.add(streamData);
            }
        }

        return result;
    }

    private static class PendingWaitRequest {
        final SocketChannel clientChannel;
        final Consumer<String> stringWriter;
        final int requiredSlaves;
        final long masterOffset;
        final long timeoutMillis;
        final long startTime;
        final CountDownLatch latch;

        PendingWaitRequest(SocketChannel clientChannel, Consumer<String> stringWriter, int requiredSlaves, long masterOffset, long timeoutMillis, CountDownLatch latch) {
            this.clientChannel = clientChannel;
            this.stringWriter = stringWriter;
            this.requiredSlaves = requiredSlaves;
            this.masterOffset = masterOffset;
            this.timeoutMillis = timeoutMillis;
            this.startTime = System.currentTimeMillis();
            this.latch = latch;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PendingWaitRequest that = (PendingWaitRequest) o;
            return clientChannel.equals(that.clientChannel) &&
                   masterOffset == that.masterOffset;
        }

        @Override
        public int hashCode() {
            return Objects.hash(clientChannel, masterOffset);
        }
    }
}