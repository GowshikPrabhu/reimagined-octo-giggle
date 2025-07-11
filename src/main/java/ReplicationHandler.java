import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

public class ReplicationHandler {

    public static void performHandshake(String masterHost, int masterPort) throws IOException {
        try (Socket socket = new Socket(masterHost, masterPort)) {
            OutputStream outputStream = socket.getOutputStream();
            BufferedInputStream bufferedInputStream = new BufferedInputStream(socket.getInputStream());
            String command = RESPEncoder.encodeStringArray(Collections.singletonList("PING"));
            outputStream.write(command.getBytes(StandardCharsets.UTF_8));

            byte[] buffer = new byte[Configs.READ_BUFFER_SIZE];
            int bytesRead;
            while ((bytesRead = bufferedInputStream.read(buffer)) != -1) {
                LoggingService.logInfo(new String(buffer, 0, bytesRead));
            }
        }
    }
}
