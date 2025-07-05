import java.util.logging.*;

public class LoggingService {
    private static final Logger logger = Logger.getLogger("ServerLog");

    static {
        logger.setUseParentHandlers(false);
        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(new Formatter() {
            @Override
            public String format(LogRecord record) {
                return String.format(
                        "%17s | %s: %s%n",
                        Thread.currentThread().getName(),
                        record.getLevel().getName(),
                        record.getMessage());
            }
        });
        handler.setLevel(Level.FINE);
        logger.addHandler(handler);
        logger.setLevel(Level.ALL);
    }

    public static void logInfo(String message) {
        logger.log(Level.INFO, message);
    }

    public static void logError(String message, Throwable throwable) {
        logger.log(Level.SEVERE, message, throwable);
    }

    public static void logError(String message) {
        logger.log(Level.SEVERE, message);
    }

    public static void logFine(String message) {
        logger.log(Level.FINE, message);
    }
}
