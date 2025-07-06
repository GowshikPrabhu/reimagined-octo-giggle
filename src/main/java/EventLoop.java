import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class EventLoop implements AutoCloseable {

    private final Selector selector;
    private final CommandParser commandParser;
    private final CommandExecutor commandExecutor;
    private final Expiry expiry;

    private final Map<SocketChannel, ByteBuffer> clientReadBuffers = new HashMap<>();
    private final Map<SocketChannel, Queue<ByteBuffer>> clientWriteBuffers = new HashMap<>();

    public EventLoop(int port) throws IOException {
        commandParser = new CommandParser();
        commandExecutor = new CommandExecutor();
        expiry = new Expiry();

        selector = Selector.open();
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.bind(new InetSocketAddress(port));
        serverSocketChannel.configureBlocking(false);
        LoggingService.logInfo("Server listening on port " + port);
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
    }

    public void start() throws IOException {
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
        SocketChannel clientChannel = (SocketChannel) key.channel();
        ByteBuffer readBuffer = clientReadBuffers.get(clientChannel);

        if (readBuffer == null) {
            LoggingService.logError("Client read buffer is null");
            closeChannel(key);
            return;
        }

        int bytesRead;
        try {
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
                List<String> cmdAndArgs = commandParser.parseNextCommand(readBuffer);

                if (cmdAndArgs == null) {
                    readBuffer.compact();
                    return;
                }

                String cmd = cmdAndArgs.getFirst();
                List<String> args = cmdAndArgs.subList(1, cmdAndArgs.size());

                LoggingService.logInfo(String.format("Client %s: received command '%s', args: %s",
                        clientChannel.getRemoteAddress(), cmd, args));

                String resp = commandExecutor.executeCommand(cmd, args);
                queueWrite(clientChannel, resp);
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

    private void handleWrite(SelectionKey key) throws IOException {
        SocketChannel clientChannel = (SocketChannel) key.channel();
        Queue<ByteBuffer> writeQueue = clientWriteBuffers.get(clientChannel);

        while (!writeQueue.isEmpty()) {
            ByteBuffer buffer = writeQueue.peek();
            clientChannel.write(buffer);
            if (buffer.hasRemaining()) {
                // Couldn’t write all data; leave OP_WRITE registered
                return;
            }
            writeQueue.poll();
        }

        key.interestOps(SelectionKey.OP_READ);
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
            clientReadBuffers.remove(channel);
            clientWriteBuffers.remove(channel);
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
