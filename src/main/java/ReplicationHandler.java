import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;


public class ReplicationHandler implements CommandExecutor.ReplicationNotifier {

    private final int localServerPort;
    private final Selector selector;
    private ReplicationState state = ReplicationState.IDLE;
    private SocketChannel masterChannel;

    private final Consumer<ByteBuffer> queueWriteToMasterCallback;
    private final Supplier<ByteBuffer> getMasterReadBufferCallback;
    private final CommandExecutor commandExecutor;
    private Consumer<Map.Entry<SocketChannel, ByteBuffer>> queueWriteToSlavesCallback;

    private int rdbBytesToRead = 0;
    private final ByteArrayOutputStream receivedRdbData = new ByteArrayOutputStream();
    private final Set<SocketChannel> connectedSlaves = Collections.synchronizedSet(new HashSet<>());

    private final Queue<RESPDecoder.DecodedResult> bufferedReplicationCommands = new LinkedList<>();
    private int bytesProcessedInReplication = 0;

    public ReplicationHandler(int port, Selector selector,
                              Consumer<ByteBuffer> queueWriteToMasterCallback,
                              Supplier<ByteBuffer> getMasterReadBufferCallback,
                              CommandExecutor commandExecutor) {
        this.localServerPort = port;
        this.selector = selector;
        this.queueWriteToMasterCallback = queueWriteToMasterCallback;
        this.getMasterReadBufferCallback = getMasterReadBufferCallback;
        this.commandExecutor = commandExecutor;
    }

    public void setQueueWriteToSlavesCallback(Consumer<Map.Entry<SocketChannel, ByteBuffer>> callback) {
        this.queueWriteToSlavesCallback = callback;
    }

    @Override
    public void registerSlaveChannel(SocketChannel slaveChannel) {
        connectedSlaves.add(slaveChannel);
        LoggingService.logInfo("Registered new slave: " + slaveChannel);
    }

    @Override
    public void removeConnectedSlave(SocketChannel slaveChannel) {
        if (connectedSlaves.contains(slaveChannel)) {
            connectedSlaves.remove(slaveChannel);
            LoggingService.logInfo("Removed slave from replication: " + slaveChannel);
        }
    }

    @Override
    public void replicateCommand(List<String> commandParts) {
        if ("master".equalsIgnoreCase(Configs.getReplicationInfo("role"))) {
            String encodedCommand = RESPEncoder.encodeStringArray(commandParts);
            ByteBuffer buffer = ByteBuffer.wrap(encodedCommand.getBytes(StandardCharsets.UTF_8));

            for (SocketChannel slaveChannel : connectedSlaves) {
                try {
                    if (queueWriteToSlavesCallback != null) {
                        queueWriteToSlavesCallback.accept(new AbstractMap.SimpleEntry<>(slaveChannel, buffer.duplicate()));
                    } else {
                        LoggingService.logWarn("queueWriteToSlavesCallback is not set. Cannot replicate command.");
                    }
                } catch (Exception e) {
                    LoggingService.logError("Error replicating command to slave " + slaveChannel + ": " + e.getMessage(), e);
                }
            }
        }
    }

    @Override
    public int getReplicationOffset() {
        return bytesProcessedInReplication;
    }

    public SocketChannel initiateHandshake() throws IOException {
        String role = Configs.getReplicationInfo("role");
        if (!"slave".equalsIgnoreCase(role)) {
            LoggingService.logInfo("Server is not configured as a slave. Skipping replication handshake.");
            return null;
        }

        String masterHost = Configs.getConfiguration("master_host");
        String masterPortStr = Configs.getConfiguration("master_port");
        if (masterHost == null || masterHost.isEmpty() || masterPortStr == null || masterPortStr.isEmpty()) {
            LoggingService.logError("Master host or port not configured for slave role. Skipping handshake.", null);
            throw new IOException("Master configuration missing for slave role.");
        }
        int masterPort;
        try {
            masterPort = Integer.parseInt(masterPortStr);
        } catch (NumberFormatException e) {
            LoggingService.logError("Invalid master port number configured: " + masterPortStr, e);
            throw new IOException("Invalid master port number: " + masterPortStr, e);
        }

        LoggingService.logInfo("Attempting replication handshake with master at " + masterHost + ":" + masterPort);
        masterChannel = SocketChannel.open();
        masterChannel.configureBlocking(false);

        boolean connected = masterChannel.connect(new InetSocketAddress(masterHost, masterPort));

        if (connected) {
            LoggingService.logInfo("Immediately connected to master: " + masterHost + ":" + masterPort);
            masterChannel.register(selector, SelectionKey.OP_READ);
            state = ReplicationState.CONNECTED_AND_READY_TO_SEND_PING;
            sendNextHandshakeCommand();
        } else {
            masterChannel.register(selector, SelectionKey.OP_CONNECT);
            state = ReplicationState.CONNECTING;
            LoggingService.logInfo("Initiated non-blocking connection to master. Waiting for OP_CONNECT.");
        }
        return masterChannel;
    }

    public void onConnected(SocketChannel channel) {
        if (channel != masterChannel) {
            LoggingService.logWarn("onConnected called for a channel that is not the master channel.");
            return;
        }
        LoggingService.logInfo("Master connection established successfully.");
        state = ReplicationState.CONNECTED_AND_READY_TO_SEND_PING;
        sendNextHandshakeCommand();
    }

    public void onConnectionFailed(SocketChannel channel, IOException e) {
        if (channel != masterChannel) {
            LoggingService.logWarn("onConnectionFailed called for a channel that is not the master channel.");
            return;
        }
        LoggingService.logError("Failed to connect to master: " + e.getMessage(), e);
        state = ReplicationState.ERROR;
    }

    public void onRead(SocketChannel channel, ByteBuffer buffer) {
        if (channel != masterChannel) {
            LoggingService.logWarn("onRead called for a channel that is not the master channel.");
            return;
        }

        try {
            if (state == ReplicationState.READING_RDB_BINARY) {
                handleRdbBinaryRead(buffer);
                if (state == ReplicationState.READY_FOR_REPLICATION) {
                    processBufferedAndRemainingCommands(buffer);
                }
                return;
            }

            if (state == ReplicationState.AWAITING_RDB_BULK_STRING_HEADER) {
                if (buffer.hasRemaining() && (char) buffer.get(buffer.position()) == '$') {
                    String bulkStringHeader = readLine(buffer);
                    if (bulkStringHeader == null) {
                        return;
                    }
                    try {
                        rdbBytesToRead = Integer.parseInt(bulkStringHeader.substring(1, bulkStringHeader.length() - 2));
                        LoggingService.logInfo("Received RDB bulk string header. RDB size: " + rdbBytesToRead + " bytes.");
                        state = ReplicationState.READING_RDB_BINARY;
                        handleRdbBinaryRead(buffer);
                        if (state == ReplicationState.READY_FOR_REPLICATION) {
                            processBufferedAndRemainingCommands(buffer);
                        }
                        return;
                    } catch (NumberFormatException | IndexOutOfBoundsException e) {
                        throw new IOException("Malformed RDB bulk string length: '" + bulkStringHeader.trim() + "'", e);
                    }
                }
            }

            while (buffer.hasRemaining()) {
                buffer.mark();
                RESPDecoder.DecodedResult decoded = RESPDecoder.decode(buffer);

                Object decodedMessage = decoded.value;

                if (decodedMessage == null) {
                    buffer.reset();
                    break;
                }

                if (state != ReplicationState.READY_FOR_REPLICATION) {
                    if (decodedMessage instanceof List) {
                        @SuppressWarnings("unchecked")
                        List<String> cmdAndArgs = (List<String>) decodedMessage;

                        if (!isHandshakeResponse(decodedMessage)) {
                            LoggingService.logInfo(String.format("Slave: Buffering command '%s' (received during handshake/RDB phase).", cmdAndArgs.getFirst()));
                            bufferedReplicationCommands.add(decoded);
                        }
                    }

                    if (decodedMessage instanceof String rawResponse) {
                        LoggingService.logInfo("Received raw response from master (" + state + "): " + rawResponse.trim());

                        switch (state) {
                            case SENT_PING:
                                if (rawResponse.equals("PONG")) {
                                    LoggingService.logInfo("Received PONG from master.");
                                    state = ReplicationState.RECEIVED_PONG;
                                    sendNextHandshakeCommand();
                                } else {
                                    throw new IOException("Unexpected response to PING: " + rawResponse);
                                }
                                break;
                            case SENT_REPLCONF_PORT:
                                if (rawResponse.equals("OK")) {
                                    LoggingService.logInfo("Received OK for REPLCONF listening-port.");
                                    state = ReplicationState.RECEIVED_REPLCONF_PORT_ACK;
                                    sendNextHandshakeCommand();
                                } else {
                                    throw new IOException("Unexpected response to REPLCONF listening-port: " + rawResponse);
                                }
                                break;
                            case SENT_REPLCONF_CAPA:
                                if (rawResponse.equals("OK")) {
                                    LoggingService.logInfo("Received OK for REPLCONF capa psync2.");
                                    state = ReplicationState.RECEIVED_REPLCONF_CAPA_ACK;
                                    sendNextHandshakeCommand();
                                } else {
                                    throw new IOException("Unexpected response to REPLCONF capa psync2: " + rawResponse);
                                }
                                break;
                            case SENT_PSYNC:
                                if (rawResponse.startsWith("FULLRESYNC")) {
                                    LoggingService.logInfo("Received FULLRESYNC from master. Now awaiting RDB bulk string header.");
                                    state = ReplicationState.AWAITING_RDB_BULK_STRING_HEADER;
                                } else {
                                    throw new IOException("Unexpected response to PSYNC: " + rawResponse);
                                }
                                break;
                            default:
                                LoggingService.logWarn("Received unexpected simple string from master in state: " + state + ": " + rawResponse);
                                break;
                        }
                    } else if (!(decodedMessage instanceof List)) {
                        LoggingService.logError("Slave: Expected simple string or command array during handshake, but received: " + decodedMessage.getClass().getSimpleName() + " (" + decodedMessage + ")");
                        throw new IOException("Protocol error: Unexpected message type during handshake.");
                    }
                } else {
                    processReplicatedMessage(decoded);
                }
            }
        } catch (IOException e) {
            LoggingService.logError("Error processing master read: " + e.getMessage(), e);
            state = ReplicationState.ERROR;

            try {
                if (masterChannel != null && masterChannel.isOpen()) {
                    masterChannel.close();
                    SelectionKey key = masterChannel.keyFor(selector);
                    if (key != null) key.cancel();
                }
            } catch (IOException ex) {
                LoggingService.logError("Error closing master channel after read error", ex);
            }
        }
    }

    private void processBufferedAndRemainingCommands(ByteBuffer buffer) throws IOException {
        while (!bufferedReplicationCommands.isEmpty()) {
            RESPDecoder.DecodedResult decoded = bufferedReplicationCommands.poll();
            @SuppressWarnings("unchecked")
            List<String> cmdAndArgs = (List<String>) decoded.value;
            LoggingService.logInfo(String.format("Slave: Applying buffered command '%s'.", cmdAndArgs.getFirst()));
            String cmd = cmdAndArgs.getFirst().toLowerCase();
            List<String> args = cmdAndArgs.subList(1, cmdAndArgs.size());
            commandExecutor.executeCommand(null, cmd, args,
                    (_) -> LoggingService.logInfo("Slave: Suppressing string response for buffered replicated cmd."),
                    (_) -> LoggingService.logInfo("Slave: Suppressing binary response for buffered replicated cmd."));
            bytesProcessedInReplication += decoded.bytesProcessed;
        }
        LoggingService.logInfo("All buffered commands applied. Replication handshake completed successfully!");

        while (buffer.hasRemaining()) {
            buffer.mark();
            RESPDecoder.DecodedResult decoded = RESPDecoder.decode(buffer);
            Object decodedMessage = decoded.value;
            if (decodedMessage != null) {
                processReplicatedMessage(decoded);
            } else {
                buffer.reset();
                break;
            }
        }
    }

    private void processReplicatedMessage(RESPDecoder.DecodedResult decoded) throws IOException {
        if (decoded.value instanceof List) {
            @SuppressWarnings("unchecked")
            List<String> cmdAndArgs = (List<String>) decoded.value;
            if (cmdAndArgs.isEmpty()) {
                LoggingService.logWarn("Slave: Received empty command array from master.");
                return;
            }

            String cmd = cmdAndArgs.getFirst().toLowerCase();
            List<String> args = cmdAndArgs.subList(1, cmdAndArgs.size());

            LoggingService.logInfo(String.format("Slave: Processing replicated command '%s', args: %s", cmd, args));

            if (cmd.equals("replconf") && !args.isEmpty() && args.getFirst().equalsIgnoreCase("getack")) {
                commandExecutor.executeCommand(null, cmd, args,
                    (resp) -> queueWriteToMasterCallback.accept(ByteBuffer.wrap(resp.getBytes(StandardCharsets.UTF_8))),
                    (_) -> {}
                );
            } else {
                commandExecutor.executeCommand(null, cmd, args,
                    (_) -> LoggingService.logInfo("Slave: Suppressing string response for replicated cmd."),
                    (_) -> LoggingService.logInfo("Slave: Suppressing binary response for replicated cmd."));
            }
            bytesProcessedInReplication += decoded.bytesProcessed;
        } else {
            LoggingService.logError("Slave: Expected command array for replication, but received: " + decoded.value.getClass().getSimpleName() + " (" + decoded.value + ")");
            throw new IOException("Protocol error: Expected array command during replication.");
        }
    }


    private void handleRdbBinaryRead(ByteBuffer buffer) throws IOException {
        if (rdbBytesToRead <= 0) {
            LoggingService.logError("handleRdbBinaryRead called with invalid rdbBytesToRead: " + rdbBytesToRead, null);
            state = ReplicationState.ERROR;
            throw new IOException("Invalid RDB size for reading.");
        }

        int bytesRemainingInRdb = rdbBytesToRead - receivedRdbData.size();
        int bytesToReadThisPass = Math.min(buffer.remaining(), bytesRemainingInRdb);

        if (bytesToReadThisPass > 0) {
            byte[] tempBytes = new byte[bytesToReadThisPass];
            buffer.get(tempBytes);
            receivedRdbData.write(tempBytes);
            LoggingService.logInfo("Read " + bytesToReadThisPass + " bytes of RDB. Total received: " + receivedRdbData.size() + "/" + rdbBytesToRead);
        }

        if (receivedRdbData.size() == rdbBytesToRead) {
            LoggingService.logInfo("Full RDB data received! Size: " + rdbBytesToRead + " bytes.");
            byte[] rdbFileContent = receivedRdbData.toByteArray();
            LoggingService.logInfo("RDB content (first 50 bytes): " + HexFormat.of().formatHex(rdbFileContent, 0, Math.min(rdbFileContent.length, 50)));

            state = ReplicationState.RECEIVED_RDB;
            state = ReplicationState.READY_FOR_REPLICATION;
            receivedRdbData.reset();
            rdbBytesToRead = 0;
        }
    }

    public void onWriteCompleted(SocketChannel channel) {
        if (channel != masterChannel) {
            LoggingService.logWarn("onWriteCompleted called for a channel that is not the master channel.");
        }
    }

    public void onReadError(SocketChannel channel, IOException e) {
        if (channel != masterChannel) {
            LoggingService.logWarn("onReadError called for a channel that is not the master channel.");
            return;
        }
        LoggingService.logError("Error during master read operation: " + e.getMessage(), e);
        state = ReplicationState.ERROR;
    }

    public void onMasterDisconnected(SocketChannel channel) {
        if (channel != masterChannel) {
            LoggingService.logWarn("onMasterDisconnected called for a channel that is not the master channel.");
            return;
        }
        LoggingService.logInfo("Master disconnected unexpectedly.");
        state = ReplicationState.ERROR;
    }

    private void sendNextHandshakeCommand() {
        try {
            switch (state) {
                case CONNECTED_AND_READY_TO_SEND_PING:
                    sendCommand(Collections.singletonList("PING"), "PING");
                    state = ReplicationState.SENT_PING;
                    break;
                case RECEIVED_PONG:
                    List<String> replconfPortCmd = List.of("REPLCONF", "listening-port", String.valueOf(localServerPort));
                    sendCommand(replconfPortCmd, "REPLCONF listening-port");
                    state = ReplicationState.SENT_REPLCONF_PORT;
                    break;
                case RECEIVED_REPLCONF_PORT_ACK:
                    List<String> replconfCapaCmd = List.of("REPLCONF", "capa", "psync2");
                    sendCommand(replconfCapaCmd, "REPLCONF capa psync2");
                    state = ReplicationState.SENT_REPLCONF_CAPA;
                    break;
                case RECEIVED_REPLCONF_CAPA_ACK:
                    List<String> psyncCmd = List.of("PSYNC", "?", "-1");
                    sendCommand(psyncCmd, "PSYNC ? -1");
                    state = ReplicationState.SENT_PSYNC;
                    break;
                case RECEIVED_RDB:
                case READY_FOR_REPLICATION:
                    break;
                case ERROR:
                    LoggingService.logError("Handshake in error state, not sending commands.", null);
                    break;
                default:
                    LoggingService.logWarn("sendNextHandshakeCommand called in unexpected state: " + state);
                    break;
            }
        } catch (IOException e) {
            LoggingService.logError("Failed to send handshake command: " + e.getMessage(), e);
            state = ReplicationState.ERROR;
        }
    }

    private void sendCommand(List<String> commandParts, String commandDescription) throws IOException {
        String encodedCommand = RESPEncoder.encodeStringArray(commandParts);
        queueWriteToMasterCallback.accept(ByteBuffer.wrap(encodedCommand.getBytes(StandardCharsets.UTF_8)));
        LoggingService.logInfo("Queued '" + commandDescription + "' command to master.");
    }

    private String readLine(ByteBuffer buffer) {
        StringBuilder sb = new StringBuilder();
        int initialPos = buffer.position();
        final int MAX_LINE_LENGTH = Configs.READ_BUFFER_SIZE * 2;

        while (buffer.hasRemaining()) {
            char c = (char) buffer.get();
            sb.append(c);
            if (sb.length() >= 2 && sb.substring(sb.length() - 2).equals("\r\n")) {
                return sb.toString();
            }
            if (buffer.position() - initialPos > MAX_LINE_LENGTH) {
                buffer.position(initialPos);
                return null;
            }
        }
        buffer.position(initialPos);
        return null;
    }

    private boolean isHandshakeResponse(Object decodedMessage) {
        if (decodedMessage instanceof String s) {
            return s.equals("PONG") || s.equals("OK") || s.startsWith("FULLRESYNC");
        }
        return false;
    }
}
