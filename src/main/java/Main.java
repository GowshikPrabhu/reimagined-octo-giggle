import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class Main {

    public static void main(String[] args) {
        LoggingService.logInfo("Logs from your program will appear here!");

        try (ServerSocket serverSocket = new ServerSocket(6379)) {
            serverSocket.setReuseAddress(true);
            LoggingService.logInfo("Server listening on port 6379");

            ThreadFactory factory = Thread.ofVirtual().name("ClientRequest-", 0).factory();
            try (ExecutorService executorService = Executors.newThreadPerTaskExecutor(factory)) {
                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    executorService.submit(new RequestHandler(clientSocket));
                }
            }
        } catch (IOException e) {
            LoggingService.logError("IOException in server: " + e.getMessage(), e);
        }
    }
}