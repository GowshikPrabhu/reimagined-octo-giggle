import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.*;

public class LoggingService {
    private static final Logger logger = Logger.getLogger("ServerLog");

    static {
        logger.setUseParentHandlers(false);
        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(new Formatter() {
            @Override
            public String format(LogRecord record) {
                StringBuilder sb = new StringBuilder();
                sb.append(String.format("%s: %s%n", record.getLevel().getName(), record.getMessage()));

                if (record.getThrown() != null) {
                    StringWriter sw = new StringWriter();
                    PrintWriter pw = new PrintWriter(sw);
                    record.getThrown().printStackTrace(pw);
                    sb.append(sw);
                }

                return sb.toString();

            }
        });
        handler.setLevel(Level.INFO);
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
