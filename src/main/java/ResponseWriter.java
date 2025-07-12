public interface ResponseWriter {
    void write(String response);

    void write(byte[] response);
}
