import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

public class ReplicationHandler {

    private final int localServerPort;

    public ReplicationHandler(int port) {
        localServerPort = port;
    }

    public void handle() throws IOException {
        String role = Configs.getReplicationInfo("role");
        if ("slave".equalsIgnoreCase(role)) {
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
            performHandshake(masterHost, masterPort);
        } else {
            LoggingService.logInfo("Server is not configured as a slave. Skipping replication handshake.");
        }
    }

    public void performHandshake(String masterHost, int masterPort) throws IOException {
        LoggingService.logInfo("Attempting replication handshake with master at " + masterHost + ":" + masterPort);
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(masterHost, masterPort), Configs.HANDSHAKE_TIMEOUT_MS);
            socket.setSoTimeout(Configs.HANDSHAKE_TIMEOUT_MS);

            OutputStream outputStream = socket.getOutputStream();
            BufferedInputStream inputStream = new BufferedInputStream(socket.getInputStream());

            sendCommandAndExpectRawResponse(outputStream, inputStream, Collections.singletonList("PING"), "PING");

            List<String> replconfPortCmd = List.of("REPLCONF", "listening-port", String.valueOf(localServerPort));
            sendCommandAndExpectRawResponse(outputStream, inputStream, replconfPortCmd, "REPLCONF listening-port");

            List<String> replconfCapaCmd = List.of("REPLCONF", "capa", "psync2");
            sendCommandAndExpectRawResponse(outputStream, inputStream, replconfCapaCmd, "REPLCONF capa psync2");

            List<String> psyncCmd = List.of("PSYNC", "?", "-1");
            sendCommandAndExpectRawResponse(outputStream, inputStream, psyncCmd, "PSYNC ? -1");

            LoggingService.logInfo("Replication handshake completed successfully with master: " + masterHost + ":" + masterPort);
        } catch (IOException e) {
            LoggingService.logError("Replication handshake failed with master at " + masterHost + ":" + masterPort + ": " + e.getMessage(), e);
            throw e;
        }
    }

    private void sendCommandAndExpectRawResponse(
            OutputStream outputStream,
            BufferedInputStream inputStream,
            List<String> commandParts,
            String commandDescription) throws IOException {

        String encodedCommand = RESPEncoder.encodeStringArray(commandParts);
        outputStream.write(encodedCommand.getBytes(StandardCharsets.UTF_8));
        outputStream.flush();
        LoggingService.logInfo("Sent '" + commandDescription + "' command to master.");

        String rawResponse = readRawRESPMessage(inputStream);
        LoggingService.logInfo("Received raw response for '" + commandDescription + "': " + rawResponse.trim());
    }

    private String readRawRESPMessage(BufferedInputStream inputStream) throws IOException {
        StringBuilder sb = new StringBuilder();
        int b;
        int bytesReadTotal = 0;
        final int MAX_RESP_MESSAGE_SIZE = Configs.READ_BUFFER_SIZE * 4;

        inputStream.mark(1);
        b = inputStream.read();
        if (b == -1) {
            throw new IOException("End of stream reached before any data for RESP message.");
        }
        char typeChar = (char) b;
        sb.append(typeChar);
        bytesReadTotal++;

        if (typeChar == '+' || typeChar == '-' || typeChar == ':') {
            while ((b = inputStream.read()) != -1) {
                sb.append((char) b);
                bytesReadTotal++;
                if (sb.length() >= 2 && sb.substring(sb.length() - 2).equals("\r\n")) {
                    return sb.toString();
                }
                if (bytesReadTotal > MAX_RESP_MESSAGE_SIZE) {
                    throw new IOException("RESP line too long or malformed, exceeding " + MAX_RESP_MESSAGE_SIZE + " bytes.");
                }
            }
        } else if (typeChar == '$') {
            String lengthLine = readLine(inputStream, sb);
            int length;
            try {
                length = Integer.parseInt(lengthLine.substring(1, lengthLine.length() - 2));
            } catch (NumberFormatException | IndexOutOfBoundsException e) {
                throw new IOException("Malformed bulk string length: '" + lengthLine.trim() + "'", e);
            }

            if (length == -1) {
                return sb.toString();
            }

            byte[] dataBuffer = new byte[length + 2];
            int totalDataBytesRead = 0;
            while (totalDataBytesRead < length + 2) {
                int read = inputStream.read(dataBuffer, totalDataBytesRead, (length + 2) - totalDataBytesRead);
                if (read == -1) {
                    throw new IOException("End of stream reached while reading bulk string data.");
                }
                totalDataBytesRead += read;
                if (bytesReadTotal + totalDataBytesRead > MAX_RESP_MESSAGE_SIZE) {
                    throw new IOException("Bulk string message too long, exceeding " + MAX_RESP_MESSAGE_SIZE + " bytes.");
                }
            }
            sb.append(new String(dataBuffer, StandardCharsets.UTF_8));
            return sb.toString();
        } else if (typeChar == '*') {
            String numElementsLine = readLine(inputStream, sb);
            int numElements;
            try {
                numElements = Integer.parseInt(numElementsLine.substring(1, numElementsLine.length() - 2));
            } catch (NumberFormatException | IndexOutOfBoundsException e) {
                throw new IOException("Malformed array element count: '" + numElementsLine.trim() + "'", e);
            }

            if (numElements < 0) {
                return sb.toString();
            }

            for (int i = 0; i < numElements; i++) {
                sb.append(readRawRESPMessage(inputStream));
                if (sb.length() > MAX_RESP_MESSAGE_SIZE) {
                    throw new IOException("Array message too long, exceeding " + MAX_RESP_MESSAGE_SIZE + " bytes.");
                }
            }
            return sb.toString();
        } else {
            throw new IOException("Unknown or unsupported RESP type: '" + typeChar + "' (ASCII: " + (int)typeChar + ")");
        }
        throw new IOException("End of stream reached before full RESP message received.");
    }

    private String readLine(BufferedInputStream inputStream, StringBuilder sb) throws IOException {
        int b;
        int bytesReadTotal = 0;
        final int MAX_LINE_LENGTH = Configs.READ_BUFFER_SIZE;

        while ((b = inputStream.read()) != -1) {
            sb.append((char) b);
            bytesReadTotal++;
            if (sb.length() >= 2 && sb.substring(sb.length() - 2).equals("\r\n")) {
                return sb.toString();
            }
            if (bytesReadTotal > MAX_LINE_LENGTH) {
                throw new IOException("RESP line too long, exceeding " + MAX_LINE_LENGTH + " bytes.");
            }
        }
        throw new IOException("End of stream reached while reading RESP line.");
    }
}