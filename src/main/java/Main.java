public class Main {

    public static void main(String[] args) {
        LoggingService.logInfo("Logs from your program will appear here!");

        try (EventLoop eventLoop = new EventLoop(6379)) {
            eventLoop.start();
        } catch (Exception e) {
            LoggingService.logError("IOException in setup: " + e.getMessage(), e);
        }
    }
}