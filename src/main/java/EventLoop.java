import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

public class EventLoop implements AutoCloseable {

    private final Selector selector;
    private final CommandParser commandParser = new CommandParser();
    private final CommandExecutor commandExecutor = new CommandExecutor();
    private final ByteBuffer readBuffer = ByteBuffer.allocate(1024);

    public EventLoop(int port) throws IOException {
        selector = Selector.open();
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.bind(new InetSocketAddress(port));
        serverSocketChannel.configureBlocking(false);
        LoggingService.logInfo("Server listening on port " + port);
        serverSocketChannel.register(selector, serverSocketChannel.validOps());
    }

    public void start() throws IOException {
        LoggingService.logInfo("Starting event loop...");

        while (true) {
            int readyCount = selector.select();

            if (readyCount == 0) {
                continue;
            }

            for (Iterator<SelectionKey> iterator = selector.selectedKeys().iterator(); iterator.hasNext(); ) {
                var key = iterator.next();
                iterator.remove();

                try {
                    if (key.isAcceptable()) {
                        handleAccept(key);
                    } else if (key.isReadable()) {
                        handleRead(key);
                    }
                } catch (IOException e) {
                    LoggingService.logError("IOException in event loop: " + e.getMessage(), e);
                    key.cancel();
                    try {
                        key.channel().close();
                    } catch (IOException e1) {
                        LoggingService.logError("Error closing channel: " + e1.getMessage(), e1);
                    }
                }
            }

        }
    }

    public void handleAccept(SelectionKey key) throws IOException {
        ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
        SocketChannel clientChannel = serverChannel.accept();
        try {
            clientChannel.configureBlocking(false);
            clientChannel.register(selector, SelectionKey.OP_READ);
            LoggingService.logInfo("Client connected: " + clientChannel.getRemoteAddress());
        } catch (IOException e) {
            LoggingService.logError("IOException in accept: " + e.getMessage(), e);
        }
    }

    public void handleRead(SelectionKey key) throws IOException {
        SocketChannel clientChannel = (SocketChannel) key.channel();
        readBuffer.clear();

        int read = clientChannel.read(readBuffer);

        if (read == -1) {
            LoggingService.logInfo("EOF reached (client disconnected).");
            key.cancel();
            clientChannel.close();
            return;
        }

        if (read > 0) {
            try {
                readBuffer.flip();
                List<String> cmdAndArgs = commandParser.parseCommand(readBuffer);

                String cmd = cmdAndArgs.getFirst();
                List<String> args = cmdAndArgs.subList(1, cmdAndArgs.size());
                LoggingService.logInfo("Parsed command: '" + cmd + "', args: " + args);

                String resp = commandExecutor.executeCommand(cmd, args);

                clientChannel.write(ByteBuffer.wrap(resp.getBytes(StandardCharsets.UTF_8)));
            } catch (IOException e) {
                LoggingService.logError("Protocol parsing error: " + e.getMessage(), e);

                String resp = RESPEncoder.encodeError("-ERR protocol error: " + e.getMessage());
                clientChannel.write(ByteBuffer.wrap(resp.getBytes(StandardCharsets.UTF_8)));
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (selector != null) {
            selector.close();
        }
    }
}
