import java.nio.channels.SocketChannel;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

public class PendingWaitRequest {
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
