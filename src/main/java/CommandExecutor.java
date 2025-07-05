import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class CommandExecutor {

    public void executeCommand(String command, List<String> args, OutputStream outputStream) throws IOException {
        if ("ping".equalsIgnoreCase(command)) {
            writePong(outputStream);
        } else if ("command".equalsIgnoreCase(command)) {
            writeAvailableCommands(outputStream);
        }else {
            LoggingService.logError("Unknown command: " + command + " with args: " + args);
            outputStream.write(("-ERR unknown command '" + command + "'\r\n").getBytes());
        }
    }

    private void writeAvailableCommands(OutputStream outputStream) throws IOException {
        List<String> commands = List.of("ping", "command");
        outputStream.write(RESPEncoder.encodeStringArray(commands).getBytes());
        outputStream.flush();
        LoggingService.logFine("Sent command list COMMAND.");
    }

    private void writePong(OutputStream outputStream) throws IOException {
        outputStream.write(RESPEncoder.encodeSimpleString("PONG").getBytes());
        outputStream.flush();
        LoggingService.logFine("Sent PONG for PING.");
    }
}
