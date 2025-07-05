import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;

public class RequestHandler implements Runnable {

    private final Socket clientSocket;
    private static final CommandParser commandParser = new CommandParser();
    private static final CommandExecutor commandExecutor = new CommandExecutor();

    public RequestHandler(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
        try {
            handleClient();
        } catch (IOException e) {
            LoggingService.logError("Error handling client request: " + e.getMessage(), e);
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                LoggingService.logError("Failure in closing client socket" + e.getMessage(), e);
            }
        }
    }

    private void handleClient() throws IOException {
        LoggingService.logInfo("Client connected: " + clientSocket.getInetAddress().getHostAddress());

        OutputStream outputStream = clientSocket.getOutputStream();
        BufferedInputStream inputStream = new BufferedInputStream(clientSocket.getInputStream());

        byte[] byteArray = new byte[1024];
        int read;
        while ((read = inputStream.read(byteArray)) != -1) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(byteArray, 0, read);

            try {
                List<String> cmdAndArgs = commandParser.parseCommand(byteBuffer);
                String cmd = cmdAndArgs.getFirst();
                List<String> args = cmdAndArgs.subList(1, cmdAndArgs.size());
                LoggingService.logInfo("Parsed command: '" + cmd + "', args: " + args);
                commandExecutor.executeCommand(cmd, args, outputStream);
            } catch (IOException e) {
                LoggingService.logError("Protocol parsing error: " + e.getMessage(), e);
                outputStream.write(RESPEncoder.encodeError("-ERR protocol error: " + e.getMessage()).getBytes());
                outputStream.flush();
            }

            byteArray = new byte[1024];
            byteBuffer.clear();
        }

        LoggingService.logInfo("EOF reached (client disconnected).");
    }
}
