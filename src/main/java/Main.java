import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class Main {

    static int port = 6379;

    public static void main(String[] args) throws IOException {
        LoggingService.logInfo("Logs from your program will appear here!");

        processArguments(args);

        try (EventLoop eventLoop = new EventLoop(port)) {
            eventLoop.start();
        } catch (Exception e) {
            LoggingService.logError("Error: " + e.getMessage(), e);
        }
    }

    private static void processArguments(String[] args) throws IOException {
        Map<String, String> argsMap = new HashMap<>();
        for (int i = 0, len = args.length; i < len; i++) {
            String arg = args[i];
            switch (arg) {
                case "--dir" -> {
                    if (++i >= len) {
                        LoggingService.logError("Missing dir location argument after --dir");
                        throw new IllegalArgumentException("Missing dir location argument after --dir");
                    }
                    String dirLocation = args[i];
                    Configs.setConfiguration("dir", dirLocation);
                    argsMap.put("dir", dirLocation);
                }
                case "--dbfilename" -> {
                    if (++i >= len) {
                        LoggingService.logError("Missing db filename argument after --dbfilename");
                        throw new IllegalArgumentException("Missing db filename argument after --dbfilename");
                    }
                    String dbFileName = args[i];
                    Configs.setConfiguration("dbfilename", dbFileName);
                    argsMap.put("dbfilename", dbFileName);
                }
                case "--port" -> {
                    if (++i >= len) {
                        LoggingService.logError("Missing port argument after --port");
                        throw new IllegalArgumentException("Missing port argument after --port");
                    }
                    String portStr = args[i];
                    try {
                        port = Integer.parseInt(portStr);
                    } catch (NumberFormatException e) {
                        LoggingService.logError("Invalid port number: " + portStr);
                    }
                }
                case "--replicaof" -> {
                    if (++i >= len) {
                        LoggingService.logError("Missing replicaof argument after --replicaof");
                        throw new IllegalArgumentException("Missing replicaof argument after --replicaof");
                    }
                    String replicaOf = args[i];
                    String[] parts = replicaOf.split(" ");
                    if (parts.length != 2) {
                        LoggingService.logError("Invalid replicaof argument: " + replicaOf);
                        throw new IllegalArgumentException("Invalid replicaof argument: " + replicaOf);
                    }
                    Configs.setReplicationInfo("role", "slave");
                    argsMap.put("replicaof", replicaOf);
                }
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

        if (!argsMap.containsKey("replicaof")) {
            Configs.setReplicationInfo("role", "master");
            Configs.setReplicationInfo("master_replid", "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb");
            Configs.setReplicationInfo("master_repl_offset", String.valueOf(0));
        }
    }
}