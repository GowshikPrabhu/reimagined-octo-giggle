import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class CommandParser {

    public List<String> parseNextCommand(ByteBuffer inputBuffer) throws IOException {
        inputBuffer.mark();

        try {
            if (!inputBuffer.hasRemaining()) {
                inputBuffer.reset();
                return null;
            }

            byte b = inputBuffer.get(inputBuffer.position());
            if (b == '*') {
                return parseRESPCommand(inputBuffer);
            } else {
                return parseInlineCommand(inputBuffer);
            }
        } catch (BufferUnderflowException e) {
            inputBuffer.reset();
            return null;
        }
    }

    private List<String> parseRESPCommand(ByteBuffer inputBuffer) throws IOException {
        if (inputBuffer.get() != '*') {
            throw new IOException("Malformed RESP: Expected '*' array prefix.");
        }

        long numElements = readRESPInteger(inputBuffer);
        if (numElements == -1) {
            inputBuffer.reset();
            return null;
        }

        List<String> commandParts = new ArrayList<>();
        for (int i = 0; i < numElements; i++) {
            if (!inputBuffer.hasRemaining()) {
                inputBuffer.reset();
                return null;
            }
            if (inputBuffer.get() != '$') {
                throw new IOException("Malformed RESP: Expected '$' bulk string prefix.");
            }

            String bulkString = parseBulkString(inputBuffer);
            if (bulkString == null) {
                inputBuffer.reset();
                return null;
            }
            commandParts.add(bulkString);
        }
        return commandParts;
    }

    private List<String> parseInlineCommand(ByteBuffer inputBuffer) throws IOException {
        String line = readLine(inputBuffer);
        if (line == null) {
            inputBuffer.reset();
            return null;
        }

        String[] parts = line.trim().split("\\s+");
        return new ArrayList<>(List.of(parts));
    }

    private String readLine(ByteBuffer byteBuffer) throws IOException {
        int initialPos = byteBuffer.position();
        StringBuilder sb = new StringBuilder();
        try {
            while (true) {
                if (!byteBuffer.hasRemaining()) {
                    byteBuffer.position(initialPos);
                    return null;
                }
                byte b = byteBuffer.get();
                if (b == '\r') {
                    if (!byteBuffer.hasRemaining()) {
                        byteBuffer.position(initialPos);
                        return null;
                    }
                    if (byteBuffer.get() == '\n') {
                        return sb.toString();
                    } else {
                        throw new IOException("Malformed RESP: Expected \\n after \\r.");
                    }
                } else {
                    sb.append((char) b);
                }
            }
        } catch (BufferUnderflowException e) {
            byteBuffer.position(initialPos);
            return null;
        }
    }

    private long readRESPInteger(ByteBuffer byteBuffer) throws IOException {
        String numStr = readLine(byteBuffer);
        if (numStr == null) {
            return -1;
        }
        try {
            return Long.parseLong(numStr);
        } catch (NumberFormatException e) {
            throw new IOException("Malformed RESP integer: " + numStr, e);
        }
    }

    private String parseBulkString(ByteBuffer byteBuffer) throws IOException {
        long length = readRESPInteger(byteBuffer);
        if (length == -1) {
            return null;
        }

        if (byteBuffer.remaining() < length + 2) {
            return null;
        }

        byte[] buffer = new byte[(int) length];
        byteBuffer.get(buffer, 0, (int) length);

        byte cr = byteBuffer.get();
        byte lf = byteBuffer.get();
        if (cr != '\r' || lf != '\n') {
            throw new IOException("Malformed bulk string: Missing or invalid trailing \\r\\n after data.");
        }
        return new String(buffer, StandardCharsets.UTF_8);
    }
}
