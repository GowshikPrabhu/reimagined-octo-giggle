import java.util.List;

public class CommandExecutor {

    public String executeCommand(String command, List<String> args) {
        if ("ping".equalsIgnoreCase(command)) {
            return getPongResponse();
        } else if ("command".equalsIgnoreCase(command)) {
            return getAvailableCommands();
        } else {
            LoggingService.logError("Unknown command: " + command + " with args: " + args);
            return "-ERR unknown command '" + command + "'\r\n";
        }
    }

    private String getAvailableCommands() {
        List<String> commands = List.of("ping", "command");
        LoggingService.logFine("Sending command list COMMAND.");
        return RESPEncoder.encodeStringArray(commands);
    }

    private String getPongResponse() {
        LoggingService.logFine("Sent PONG for PING.");
        return RESPEncoder.encodeSimpleString("PONG");
    }
}
