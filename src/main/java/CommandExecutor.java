import java.util.List;

public class CommandExecutor {

    public String executeCommand(String command, List<String> args) {
        if ("ping".equalsIgnoreCase(command)) {
            return getPongResponse();
        } else if ("command".equalsIgnoreCase(command)) {
            return getAvailableCommands();
        } else if ("echo".equalsIgnoreCase(command)) {
            return getEchoResponse(args);
        } else {
            LoggingService.logError("Unknown command: " + command + " with args: " + args);
            return "-ERR unknown command '" + command + "'\r\n";
        }
    }

    private String getEchoResponse(List<String> args) {
        if (args.isEmpty()) {
            return RESPEncoder.encodeError("ERR echo requires at least one argument");
        }
        String arg = args.getFirst();
        LoggingService.logFine("Echoing: " + arg);
        return RESPEncoder.encodeBulkString(arg);
    }

    private String getAvailableCommands() {
        List<String> commands = List.of("ping", "command", "echo");
        LoggingService.logFine("Sending command list COMMAND.");
        return RESPEncoder.encodeStringArray(commands);
    }

    private String getPongResponse() {
        LoggingService.logFine("Sent PONG for PING.");
        return RESPEncoder.encodeSimpleString("PONG");
    }
}
