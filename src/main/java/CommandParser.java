import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class CommandParser {

    public List<String> parseCommand(ByteBuffer byteBuffer) throws IOException {
        byteBuffer.get(); // Consume *

        long size = parseRESPInteger(byteBuffer);
        return parseArray(byteBuffer, size);

    }

    private String readLine(ByteBuffer byteBuffer) throws IOException {
        StringBuilder sb = new StringBuilder();
        int b;
        while ((b = byteBuffer.get()) != '\r') {
            sb.append((char) b);
        }
        if (byteBuffer.get() != '\n') {
            throw new IOException("Malformed RESP: Expected \\n after \\r.");
        }
        return sb.toString();
    }

    private long parseRESPInteger(ByteBuffer byteBuffer) throws IOException {
        String numStr = readLine(byteBuffer);
        try {
            return Long.parseLong(numStr);
        } catch (NumberFormatException e) {
            throw new IOException("Malformed RESP integer: " + numStr, e);
        }
    }

    private String parseBulkString(ByteBuffer byteBuffer) throws IOException {
        long length = parseRESPInteger(byteBuffer);

        byte[] buffer = new byte[(int) length];
        byteBuffer.get(buffer, 0, (int) length);

        byte cr = byteBuffer.get();
        byte lf = byteBuffer.get();
        if (cr != '\r' || lf != '\n') {
            throw new IOException("Malformed bulk string: Missing or invalid trailing \\r\\n after data. Read: " + cr + "," + lf);
        }
        return new String(buffer);
    }

    private List<String> parseArray(ByteBuffer byteBuffer, long size) throws IOException {
        List<String> commandParts = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            byteBuffer.get();
            String bulkString = parseBulkString(byteBuffer);
            commandParts.add(bulkString);
        }
        return commandParts;
    }
}
