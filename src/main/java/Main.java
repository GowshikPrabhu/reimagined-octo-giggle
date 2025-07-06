import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class Main {

    public static void main(String[] args) throws IOException {
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

    private static void processArguments(String[] args) throws IOException {
        Map<String, String> argsMap = new HashMap<>();
        for (int i = 0, len = args.length; i < len; i++) {
            String arg = args[i];
            if (arg.equals("--dir")) {
                if (++i >= len) {
                    LoggingService.logError("Missing dir location argument after --dir");
                    throw new IllegalArgumentException("Missing dir location argument after --dir");
                }
                String dirLocation = args[i];
                Configs.setConfiguration("dir", dirLocation);
                argsMap.put("dir", dirLocation);
            } else if (arg.equals("--dbfilename")) {
                if (++i >= len) {
                    LoggingService.logError("Missing db filename argument after --dbfilename");
                    throw new IllegalArgumentException("Missing db filename argument after --dbfilename");
                }
                String dbFileName = args[i];
                Configs.setConfiguration("dbfilename", dbFileName);
                argsMap.put("dbfilename", dbFileName);
            }
        }

        boolean readRDB = false;
        if (argsMap.containsKey("dir")) {
            String dir = argsMap.get("dir");
            Path path = Paths.get(dir);
            if (!Files.exists(path)) {
                Files.createDirectories(path);
            }
            if (!Files.isDirectory(path)) {
                LoggingService.logError("Path " + dir + " is not a directory");
                throw new IllegalArgumentException("Path " + dir + " is not a directory");
            }
        }
        if (argsMap.containsKey("dbfilename")) {
            String dbFileName = argsMap.get("dbfilename");
            String dir = argsMap.getOrDefault("dir", ".");
            Path path = Paths.get(dir, dbFileName);
            if (!Files.exists(path)) {
                Files.createFile(path);
            } else {
                readRDB = true;
            }
            if (!Files.isRegularFile(path)) {
                LoggingService.logError("Path " + dbFileName + " is not a regular file");
                throw new IllegalArgumentException("Path " + dbFileName + " is not a regular file");
            }
        }

        if (readRDB) {
            RDBParser rdbParser = new RDBParser();
            String dir = argsMap.getOrDefault("dir", ".");
            String fileName = argsMap.getOrDefault("dbfilename", "redis.rdb");
            rdbParser.parse(Path.of(dir, fileName).toAbsolutePath());
        }
    }
}