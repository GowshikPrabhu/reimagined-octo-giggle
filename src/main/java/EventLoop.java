import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class EventLoop implements AutoCloseable {

    private final Selector selector;
    private final CommandParser commandParser;
    private final CommandExecutor commandExecutor;
    private final Expiry expiry;
    private final ReplicationHandler replicationHandler;

    private final Map<SocketChannel, ByteBuffer> clientReadBuffers = new HashMap<>();
    private final Map<SocketChannel, Queue<ByteBuffer>> clientWriteBuffers = new HashMap<>();

    private SocketChannel masterChannel = null;
    private ByteBuffer masterReadBuffer = null;
    private final Queue<ByteBuffer> masterWriteQueue = new LinkedList<>();


    public EventLoop(int port) throws IOException {
        commandParser = new CommandParser();
        commandExecutor = new CommandExecutor();
        expiry = new Expiry();
        selector = Selector.open();

        replicationHandler = new ReplicationHandler(port, selector, this::queueWriteToMaster, this::queueReadForMaster, commandExecutor);
        commandExecutor.setReplicationNotifier(replicationHandler);
        replicationHandler.setQueueWriteToSlavesCallback(entry -> queueWriteToClient(entry.getKey(), entry.getValue()));

        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.bind(new InetSocketAddress(port));
        serverSocketChannel.configureBlocking(false);
        LoggingService.logInfo("Server listening on port " + port);
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
    }

    public void start() throws IOException {
        String role = Configs.getReplicationInfoAsString("role");
        if ("slave".equalsIgnoreCase(role)) {
            try {
                masterChannel = replicationHandler.initiateHandshake();
                if (masterChannel != null) {
                    masterReadBuffer = ByteBuffer.allocate(Configs.READ_BUFFER_SIZE);
                }
            } catch (IOException e) {
                LoggingService.logError("Failed to initiate replication handshake: " + e.getMessage(), e);
            }
        } else {
            LoggingService.logInfo("Server is not configured as a slave. Skipping replication handshake.");
        }


        LoggingService.logInfo("Starting event loop...");

        while (selector.isOpen()) {
            int readyCount = selector.select(Configs.SELECTOR_WAIT_INTERVAL_MS);

            expiry.scanAndExpire();

            if (readyCount > 0) {
                processSelectedKeys();
            }
        }
    }

    private void processSelectedKeys() {
        Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();

        while (iterator.hasNext()) {
            SelectionKey key = iterator.next();
            iterator.remove();

            if (!key.isValid()) {
                continue;
            }

            try {
                if (key.isAcceptable()) {
                    handleAccept(key);
                } else if (key.isConnectable()) {
                    handleConnect(key);
                } else if (key.isReadable()) {
                    handleRead(key);
                } else if (key.isWritable()) {
                    handleWrite(key);
                }
            } catch (IOException e) {
                LoggingService.logError("I/O error on channel operation", e);
                closeChannel(key);
            } catch (Exception e) {
                LoggingService.logError("Unexpected error handling key", e);
                closeChannel(key);
            }
        }
    }

    private void handleConnect(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        try {
            if (channel.isConnectionPending()) {
                channel.finishConnect();
            }
            LoggingService.logInfo("Connection established with " + channel.getRemoteAddress());
            key.interestOps(SelectionKey.OP_READ);
            replicationHandler.onConnected(channel);
        } catch (IOException e) {
            LoggingService.logError("Failed to establish connection: " + e.getMessage(), e);
            replicationHandler.onConnectionFailed(channel, e);
            closeChannel(key);
        }
    }

    private void handleAccept(SelectionKey key) throws IOException {
        ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
        SocketChannel clientChannel = serverChannel.accept();
        if (clientChannel == null) {
            return;
        }
        try {
            clientChannel.configureBlocking(false);
            clientChannel.register(selector, SelectionKey.OP_READ);
            clientReadBuffers.put(clientChannel, ByteBuffer.allocate(Configs.READ_BUFFER_SIZE));
            clientWriteBuffers.put(clientChannel, new LinkedList<>());
            LoggingService.logInfo("Client connected: " + clientChannel.getRemoteAddress());
        } catch (IOException e) {
            LoggingService.logError("IOException in accept: " + e.getMessage(), e);
            closeSocketChannel(clientChannel);
        }
    }

    private void handleRead(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();

        if (channel == masterChannel) {
            handleMasterRead(key);
        } else {
            handleClientRead(key);
        }
    }

    private void handleClientRead(SelectionKey key) {
        SocketChannel clientChannel = (SocketChannel) key.channel();
        ByteBuffer readBuffer = clientReadBuffers.get(clientChannel);

        if (readBuffer == null) {
            LoggingService.logError("Client read buffer is null");
            closeChannel(key);
            return;
        }

        int bytesRead;
        try {
            LoggingService.logInfo("Reading from client channel: " + clientChannel.getRemoteAddress());
            bytesRead = clientChannel.read(readBuffer);
        } catch (IOException e) {
            LoggingService.logError("Error reading from client channel: " + e.getMessage(), e);
            closeChannel(key);
            return;
        }

        if (bytesRead == -1) {
            LoggingService.logInfo("EOF reached (client disconnected).");
            closeChannel(key);
            return;
        }

        readBuffer.flip();
        int commandsProcessed = 0;

        try {
            while (readBuffer.hasRemaining() && commandsProcessed++ < Configs.MAX_COMMANDS_PER_READ) {
                int startingPosition = readBuffer.position();
                List<String> cmdAndArgs = commandParser.parseNextCommand(readBuffer);

                if (cmdAndArgs == null) {
                    readBuffer.compact();
                    return;
                }
                int bytesConsumed = readBuffer.position() - startingPosition;

                String cmd = cmdAndArgs.getFirst();
                List<String> args = cmdAndArgs.subList(1, cmdAndArgs.size());

                LoggingService.logInfo(String.format("Client %s: received command '%s', args: %s",
                        clientChannel.getRemoteAddress(), cmd, args));

                Consumer<String> stringWriter = (String s) -> queueWrite(clientChannel, s);
                Consumer<byte[]> byteWriter = (byte[] b) -> queueWrite(clientChannel, b);

                commandExecutor.executeCommand(clientChannel, cmd, args, stringWriter, byteWriter, bytesConsumed);
            }
        } catch (IOException e) {
            LoggingService.logError("Protocol parsing error: " + e.getMessage(), e);
            queueWrite(clientChannel, RESPEncoder.encodeError("ERR protocol error: " + e.getMessage()));
            closeChannel(key);
        } catch (Exception e) {
            LoggingService.logError("Unexpected error during command processing: " + e.getMessage(), e);
            queueWrite(clientChannel, RESPEncoder.encodeError("ERR internal server error: " + e.getMessage()));
            closeChannel(key);
        } finally {
            if (readBuffer.hasRemaining()) {
                readBuffer.compact();
            } else {
                readBuffer.clear();
            }
        }
    }

    private void handleMasterRead(SelectionKey key) {
        SocketChannel channel = (SocketChannel) key.channel();
        int bytesRead;
        try {
            bytesRead = channel.read(masterReadBuffer);
        } catch (IOException e) {
            LoggingService.logError("Error reading from master channel: " + e.getMessage(), e);
            replicationHandler.onReadError(channel, e);
            closeChannel(key);
            return;
        }

        if (bytesRead == -1) {
            LoggingService.logInfo("EOF reached (master disconnected).");
            replicationHandler.onMasterDisconnected(channel);
            closeChannel(key);
            return;
        }

        masterReadBuffer.flip();
        replicationHandler.onRead(channel, masterReadBuffer);
        if (masterReadBuffer.hasRemaining()) {
            masterReadBuffer.compact();
        } else {
            masterReadBuffer.clear();
        }
    }

    private void handleWrite(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();

        if (channel == masterChannel) {
            handleMasterWrite(key);
        } else {
            handleClientWrite(key);
        }
    }

    private void handleClientWrite(SelectionKey key) throws IOException {
        SocketChannel clientChannel = (SocketChannel) key.channel();
        Queue<ByteBuffer> writeQueue = clientWriteBuffers.get(clientChannel);

        while (writeQueue != null && !writeQueue.isEmpty()) {
            ByteBuffer buffer = writeQueue.peek();
            clientChannel.write(buffer);
            if (buffer.hasRemaining()) {
                return;
            }
            writeQueue.poll();
        }

        key.interestOps(SelectionKey.OP_READ);
    }

    private void handleMasterWrite(SelectionKey key) throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();

        while (!masterWriteQueue.isEmpty()) {
            ByteBuffer buffer = masterWriteQueue.peek();
            channel.write(buffer);
            if (buffer.hasRemaining()) {
                return;
            }
            masterWriteQueue.poll();
        }

        key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE | SelectionKey.OP_READ);
        replicationHandler.onWriteCompleted(channel);
    }

    private void queueWrite(SocketChannel channel, String response) {
        Queue<ByteBuffer> writeQueue = clientWriteBuffers.get(channel);
        if (writeQueue == null) {
            LoggingService.logError("Write queue missing for client: " + channel);
            return;
        }

        writeQueue.add(ByteBuffer.wrap(response.getBytes(StandardCharsets.UTF_8)));

        SelectionKey key = channel.keyFor(selector);
        if (key != null && key.isValid()) {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            selector.wakeup();
        }
    }

    private void queueWrite(SocketChannel channel, byte[] response) {
        Queue<ByteBuffer> writeQueue = clientWriteBuffers.get(channel);
        if (writeQueue == null) {
            LoggingService.logError("Write queue missing for client: " + channel);
            return;
        }

        writeQueue.add(ByteBuffer.wrap(response));

        SelectionKey key = channel.keyFor(selector);
        if (key != null && key.isValid()) {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            selector.wakeup();
        }
    }

    private void queueWriteToMaster(ByteBuffer buffer) {
        if (masterChannel == null || !masterChannel.isConnected()) {
            LoggingService.logError("Attempted to queue write to master, but master channel is not connected.", null);
            return;
        }
        masterWriteQueue.add(buffer);

        SelectionKey key = masterChannel.keyFor(selector);
        if (key != null && key.isValid()) {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            selector.wakeup();
        }
    }

    private void queueWriteToClient(SocketChannel channel, ByteBuffer buffer) {
        Queue<ByteBuffer> writeQueue = clientWriteBuffers.get(channel);
        if (writeQueue == null) {
            LoggingService.logError("Write queue missing for client: " + channel);
            return;
        }

        writeQueue.add(buffer);

        SelectionKey key = channel.keyFor(selector);
        if (key != null && key.isValid()) {
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            selector.wakeup();
        }
    }

    private ByteBuffer queueReadForMaster() {
        return masterReadBuffer;
    }


    private void closeChannel(SelectionKey key) {
        SocketChannel channel = (SocketChannel) key.channel();
        key.cancel();
        closeSocketChannel(channel);
    }

    private void closeSocketChannel(SocketChannel channel) {
        try {
            LoggingService.logInfo("Closing connection: " + channel.getRemoteAddress());
        } catch (IOException ignored) {
        }

        try {
            channel.close();
        } catch (IOException e) {
            LoggingService.logError("Error closing channel", e);
        } finally {
            if (channel == masterChannel) {
                masterChannel = null;
                masterReadBuffer = null;
                masterWriteQueue.clear();
                replicationHandler.onMasterDisconnected(channel);
            } else {
                clientReadBuffers.remove(channel);
                clientWriteBuffers.remove(channel);
                replicationHandler.removeConnectedSlave(channel);
            }
        }
    }

    @Override
    public void close() throws Exception {
        LoggingService.logInfo("Shutting down event loop...");
        for (SelectionKey key : new HashSet<>(selector.keys())) {
            closeChannel(key);
        }
        selector.close();
    }
}
