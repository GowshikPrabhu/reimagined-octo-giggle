public class Main {

    public static void main(String[] args) {
        LoggingService.logInfo("Logs from your program will appear here!");

        if (args.length > 0) {
            processArguments(args);
        }

        try (EventLoop eventLoop = new EventLoop(6379)) {
            eventLoop.start();
        } catch (Exception e) {
            LoggingService.logError("Error: " + e.getMessage(), e);
        }
    }

    private static void processArguments(String[] args) {
        for (int i = 0, len = args.length; i < len; i++) {
            String arg = args[i];
            if (arg.equals("--dir")) {
                if (++i >= len) {
                    LoggingService.logError("Missing dir location argument after --dir");
                    throw new IllegalArgumentException("Missing dir location argument after --dir");
                }
                String dirLocation = args[i];
                Configs.setConfiguration("dir", dirLocation);
            } else if (arg.equals("--dbfilename")) {
                if (++i >= len) {
                    LoggingService.logError("Missing db filename argument after --dbfilename");
                    throw new IllegalArgumentException("Missing db filename argument after --dbfilename");
                }
                String dbFileName = args[i];
                Configs.setConfiguration("dbfilename", dbFileName);
            }
        }
    }
}