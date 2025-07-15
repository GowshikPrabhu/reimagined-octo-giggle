import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.function.Consumer;

/**
 * @param streamKeys STREAMS keys
 * @param streamIds  IDs to start reading from
 * @param count      Max entries to return
 * @param unblockAt  System.currentTimeMillis() + blockMillis
 */
public record BlockedClient(SocketChannel clientChannel, List<String> streamKeys, List<String> streamIds, int count,
                            long unblockAt, Consumer<String> stringWriter, Consumer<byte[]> byteWriter) {
}
