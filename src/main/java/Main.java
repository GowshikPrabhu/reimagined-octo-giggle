import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.*;

public class Main {
    private static final Logger logger = Logger.getLogger(Main.class.getName());

    static {
        logger.setUseParentHandlers(false);
        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(new Formatter() {
            @Override
            public String format(LogRecord record) {
                return String.format("%s: %s%n", record.getLevel().getName(), record.getMessage());
            }
        });
        handler.setLevel(Level.INFO);
        logger.addHandler(handler);
        logger.setLevel(Level.ALL);
    }

    public static void main(String[] args) {
        logger.info("Logs from your program will appear here!");


        try (ServerSocket serverSocket = new ServerSocket(6379)) {
            serverSocket.setReuseAddress(true);
            logger.info("Server listening on port 6379");


            try (Socket clientSocket = serverSocket.accept()) {
                logger.info("Client connected: " + clientSocket.getInetAddress().getHostAddress());

                OutputStream outputStream = clientSocket.getOutputStream();
                BufferedInputStream inputStream = new BufferedInputStream(clientSocket.getInputStream());

                int readByte;

                while ((readByte = inputStream.read()) != -1) {
                    char typeChar = (char) readByte;
                    logger.fine("\nReceived type char: '" + typeChar + "' (ASCII: " + readByte + ")");

                    try {
                        if (typeChar == '+') {
                            String val = readSimpleString(inputStream);
                            logger.info("Parsed simple string: '" + val + "'");


                            outputStream.write("-ERR unknown simple string command\r\n".getBytes());
                        } else if (typeChar == '-') {
                            String err = readSimpleString(inputStream);
                            logger.info("Parsed error string: '" + err + "'");

                        } else if (typeChar == ':') {
                            long num = readRESPInteger(inputStream);
                            logger.info("Parsed integer: " + num);

                            outputStream.write("-ERR unknown integer command\r\n".getBytes());
                        } else if (typeChar == '$') {
                            String bulkString = readBulkString(inputStream);
                            logger.info("Parsed bulk string: '" + (bulkString == null ? "NULL" : bulkString) + "'");


                            if ("ping".equalsIgnoreCase(bulkString)) {
                                writePong(outputStream);
                                logger.fine("Sent PONG for standalone bulk string PING.");
                            } else {
                                outputStream.write("-ERR unknown standalone bulk string command\r\n".getBytes());
                            }
                        } else if (typeChar == '*') {
                            long size = readRESPInteger(inputStream);
                            logger.info("Parsed array with size: " + size);

                            if (size == 0) {
                                logger.fine("Received empty array command.");
                                outputStream.write("-ERR empty array command\r\n".getBytes());
                            } else if (size < 0) {
                                logger.fine("Received null array command.");
                                outputStream.write("-ERR null array command\r\n".getBytes());
                            } else {
                                List<String> commandParts = new ArrayList<>();
                                for (int i = 0; i < size; i++) {
                                    int elementTypeByte = inputStream.read();
                                    if (elementTypeByte == -1) {
                                        throw new EOFException("Unexpected EOF while reading array element type.");
                                    }
                                    char elementTypeChar = (char) elementTypeByte;
                                    logger.fine("  Array element " + i + " type: '" + elementTypeChar + "'");

                                    if (elementTypeChar == '$') {
                                        String bulkString = readBulkString(inputStream);
                                        commandParts.add(bulkString);
                                    } else if (elementTypeChar == ':') {
                                        long num = readRESPInteger(inputStream);
                                        commandParts.add(String.valueOf(num));
                                    } else {
                                        throw new IOException("Unsupported element type in array: '" + elementTypeChar + "'");
                                    }
                                }

                                if (!commandParts.isEmpty()) {
                                    String command = commandParts.getFirst();
                                    logger.info("Parsed command: '" + command + "', args: " + commandParts.subList(1, commandParts.size()));

                                    if ("ping".equalsIgnoreCase(command)) {
                                        writePong(outputStream);
                                        logger.fine("Sent PONG for array command PING.");
                                    } else {
                                        outputStream.write(("-ERR unknown command '" + command + "'\r\n").getBytes());
                                        logger.fine("Sent ERROR for unknown command: " + command);
                                    }
                                } else {
                                    logger.fine("Array had elements but parsed empty command parts.");
                                    outputStream.write("-ERR malformed array command\r\n".getBytes());
                                }
                            }
                        } else {
                            outputStream.write(("-ERR unrecognized RESP type: '" + typeChar + "'\r\n").getBytes());
                            logger.severe("Unrecognized RESP type: '" + typeChar + "' (ASCII: " + readByte + "). Skipping to next potential command.");

                            throw new IOException("Unrecognized RESP type: " + typeChar);
                        }
                    } catch (IOException e) {
                        logger.severe("Protocol parsing error: " + e.getMessage());

                        outputStream.write(("-ERR protocol error: " + e.getMessage().replace("\n", " ").replace("\r", " ") + "\r\n").getBytes());
                        outputStream.flush();
                        throw e;
                    }
                    outputStream.flush();
                }
                logger.info("EOF reached (client disconnected).");
            }
        } catch (IOException e) {
            logger.severe("IOException in server: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void writePong(OutputStream outputStream) throws IOException {
        outputStream.write("+PONG\r\n".getBytes());
        outputStream.flush();
    }


    private static String readLine(BufferedInputStream inputStream) throws IOException {
        StringBuilder sb = new StringBuilder();
        int b;
        while ((b = inputStream.read()) != -1) {
            if (b == '\r') {
                int nextByte = inputStream.read();
                if (nextByte == -1) {
                    throw new EOFException("Unexpected EOF while reading \\n after \\r.");
                }
                if (nextByte != '\n') {
                    throw new IOException("Malformed RESP: \\r not followed by \\n. Next byte: " + nextByte);
                }
                return sb.toString();
            } else {
                sb.append((char) b);
            }
        }
        throw new EOFException("Unexpected EOF while reading line (missing \\r\\n).");
    }


    private static String readSimpleString(BufferedInputStream inputStream) throws IOException {
        return readLine(inputStream);
    }


    private static long readRESPInteger(BufferedInputStream inputStream) throws IOException {
        String numStr = readLine(inputStream);
        try {
            return Long.parseLong(numStr);
        } catch (NumberFormatException e) {
            throw new IOException("Malformed RESP integer: " + numStr, e);
        }
    }

    private static String readBulkString(BufferedInputStream inputStream) throws IOException {
        long length = readRESPInteger(inputStream);

        if (length == -1) {
            return null;
        }
        if (length < 0) {
            throw new IOException("Malformed bulk string: Negative length specified " + length);
        }

        byte[] buffer = new byte[(int) length];
        int bytesRead = 0;
        while (bytesRead < length) {
            int result = inputStream.read(buffer, bytesRead, (int) (length - bytesRead));
            if (result == -1) {
                throw new EOFException("Unexpected EOF while reading bulk string data. Read " + bytesRead + " of " + length + " bytes.");
            }
            bytesRead += result;
        }

        int cr = inputStream.read();
        int lf = inputStream.read();
        if (cr != '\r' || lf != '\n') {
            throw new IOException("Malformed bulk string: Missing or invalid trailing \\r\\n after data. Read: " + cr + "," + lf);
        }
        return new String(buffer);
    }
}