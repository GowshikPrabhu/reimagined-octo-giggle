import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RESPDecoder {

    public static class DecodedResult {
        public final Object value;
        public final int bytesProcessed;

        public DecodedResult(Object value, int bytesProcessed) {
            this.value = value;
            this.bytesProcessed = bytesProcessed;
        }
    }

    public static DecodedResult decode(ByteBuffer buffer) throws IOException {
        if (!buffer.hasRemaining()) {
            return new DecodedResult(null, 0);
        }

        int startPos = buffer.position();
        buffer.mark();
        char typeChar = (char) buffer.get();

        try {
            switch (typeChar) {
                case '+':
                case '-':
                case ':': {
                    String line = readLine(buffer);
                    if (line != null) {
                        int bytes = buffer.position() - startPos;
                        return new DecodedResult(line.substring(0, line.length() - 2), bytes);
                    }
                    break;
                }
                case '$': {
                    String lengthLine = readLine(buffer);
                    if (lengthLine == null) {
                        break;
                    }
                    int length;
                    try {
                        length = Integer.parseInt(lengthLine.substring(0, lengthLine.length() - 2));
                    } catch (NumberFormatException | IndexOutOfBoundsException e) {
                        throw new IOException("Malformed bulk string length: '" + lengthLine.trim() + "'", e);
                    }

                    if (length == -1) {
                        int bytes = buffer.position() - startPos;
                        return new DecodedResult(null, bytes);
                    }

                    if (buffer.remaining() >= length + 2) {
                        byte[] data = new byte[length];
                        buffer.get(data);
                        if (buffer.get() != '\r' || buffer.get() != '\n') {
                            throw new IOException("Missing CRLF after bulk string data.");
                        }
                        int bytes = buffer.position() - startPos;
                        return new DecodedResult(new String(data, StandardCharsets.UTF_8), bytes);
                    }
                    break;
                }
                case '*': {
                    String numElementsLine = readLine(buffer);
                    if (numElementsLine == null) {
                        break;
                    }
                    int numElements;
                    try {
                        numElements = Integer.parseInt(numElementsLine.substring(0, numElementsLine.length() - 2));
                    } catch (NumberFormatException | IndexOutOfBoundsException e) {
                        throw new IOException("Malformed array element count: '" + numElementsLine.trim() + "'", e);
                    }

                    if (numElements == -1) {
                        int bytes = buffer.position() - startPos;
                        return new DecodedResult(null, bytes);
                    }

                    List<String> array = new ArrayList<>(numElements);
                    int totalBytes = buffer.position() - startPos;
                    for (int i = 0; i < numElements; i++) {
                        DecodedResult element = decode(buffer);
                        if (element.value == null) {
                            buffer.reset();
                            return new DecodedResult(null, 0);
                        }
                        totalBytes += element.bytesProcessed;
                        if (element.value instanceof String) {
                            array.add((String) element.value);
                        } else {
                            throw new IOException("Unsupported element type in array: " + element.value.getClass().getSimpleName());
                        }
                    }
                    return new DecodedResult(array, totalBytes);
                }
                default:
                    throw new IOException("Unknown or unsupported RESP type: '" + typeChar + "' (ASCII: " + (int) typeChar + ")");
            }
        } catch (IOException e) {
            buffer.reset();
            throw e;
        }

        buffer.reset();
        return new DecodedResult(null, 0);
    }

    private static String readLine(ByteBuffer buffer) {
        StringBuilder sb = new StringBuilder();
        int bytesRead = 0;
        final int MAX_LINE_LENGTH = 1024 * 8;

        while (buffer.hasRemaining()) {
            char c = (char) buffer.get();
            sb.append(c);
            bytesRead++;
            if (sb.length() >= 2 && sb.substring(sb.length() - 2).equals("\r\n")) {
                return sb.toString();
            }
            if (bytesRead > MAX_LINE_LENGTH) {
                return null;
            }
        }
        return null;
    }
}