import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RESPDecoder {

    public static Object decode(ByteBuffer buffer) throws IOException {
        if (!buffer.hasRemaining()) {
            return null;
        }

        buffer.mark();
        char typeChar = (char) buffer.get();

        try {
            switch (typeChar) {
                case '+':
                case '-':
                case ':':
                    String line = readLine(buffer);
                    if (line != null) {
                        return line.substring(0, line.length() - 2);
                    }
                    break;
                case '$':
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
                        return null;
                    }

                    if (buffer.remaining() >= length + 2) {
                        byte[] data = new byte[length];
                        buffer.get(data);
                        if (buffer.get() != '\r' || buffer.get() != '\n') {
                            throw new IOException("Missing CRLF after bulk string data.");
                        }
                        return new String(data, StandardCharsets.UTF_8);
                    }
                    break;
                case '*':
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
                        return null;
                    }

                    List<String> array = new ArrayList<>(numElements);
                    for (int i = 0; i < numElements; i++) {
                        Object element = decode(buffer);
                        if (element == null) {
                            buffer.reset();
                            return null;
                        }
                        if (element instanceof String) {
                            array.add((String) element);
                        } else {
                            throw new IOException("Unsupported element type in array: " + element.getClass().getSimpleName());
                        }
                    }
                    return array;
                default:
                    throw new IOException("Unknown or unsupported RESP type: '" + typeChar + "' (ASCII: " + (int) typeChar + ")");
            }
        } catch (IOException e) {
            buffer.reset();
            throw e;
        }

        buffer.reset();
        return null;
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