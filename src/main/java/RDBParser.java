import com.ning.compress.lzf.LZFDecoder;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RDBParser {
    private static final int OP_AUX = 0xFA;
    private static final int OP_RESIZEDB = 0xFB;
    private static final int OP_EXPIRETIMEMS = 0xFC;
    private static final int OP_EXPIRETIME = 0xFD;
    private static final int OP_SELECTDB = 0xFE;
    private static final int OP_END = 0xFF;
    private static final int NUM_6BIT = 0b00000000;
    private static final int NUM_14BIT = 0b01000000;
    private static final int NUM_32BIT = 0b10000000;
    private static final int NUM_ENCODING = 0b11000000;
    private static final int ENC_8BIT_INT = 0;
    private static final int ENC_16BIT_INT = 1;
    private static final int ENC_32BIT_INT = 2;
    private static final int ENC_COMPRESSED = 3;

    private static final int VALUE_TYPE_STRING = 0;
    private static final int VALUE_TYPE_LIST = 1;
    private static final int VALUE_TYPE_SET = 2;
    private static final int VALUE_TYPE_SORTED_SET = 3;
    private static final int VALUE_TYPE_HASH = 4;
    private static final int VALUE_TYPE_ZIPMAP = 9;
    private static final int VALUE_TYPE_ZIPLIST = 10;
    private static final int VALUE_TYPE_INTSET = 11;
    private static final int VALUE_TYPE_SORTED_SET_ZIPLIST = 12;
    private static final int VALUE_TYPE_HASH_ZIPLIST = 13;

    int version;

    public void parse(Path filePath) {
        try (DataInputStream stream = new DataInputStream(new BufferedInputStream(Files.newInputStream(filePath)))) {
            parse(stream);
        } catch (FileNotFoundException e) {
            LoggingService.logError("File not found: " + filePath);
            throw new RuntimeException(e);
        } catch (IOException e) {
            LoggingService.logError("Error reading file: " + filePath, e);
            throw new RuntimeException(e);
        }
    }

    private void parse(DataInputStream reader) throws IOException {
        verifyMagicNumber(reader);
        readVersionNumber(reader);

        while (true) {
            int opCode = reader.read();
            checkEOF(opCode, "Reading op code");

            if (opCode == OP_AUX) {
                String key = readString(reader);
                String value = readString(reader);
                LoggingService.logInfo("Auxiliary key: " + key + " value: " + value);
            } else if (opCode == OP_SELECTDB) {
                int dbNo = getLengthAndEncoding(reader).length;
                LoggingService.logInfo("Selecting DB: " + dbNo);
            } else if (opCode == OP_RESIZEDB) {
                readResizeDBData(reader);
            } else if (opCode == OP_EXPIRETIME) {
                int expirySecs = readInt(reader);
                int valueType = reader.read();
                checkEOF(valueType, "Reading value type");
                String key = readString(reader);
                Object value = readValueByType(reader, valueType);
                Cache.getInstance().put(key, value.toString(), expirySecs * 1000L);
                LoggingService.logInfo("Setting expiry for key: " + key + " value: " + value + " expiry: " + expirySecs);
            } else if (opCode == OP_EXPIRETIMEMS) {
                long expiryMillis = readLong(reader);
                int valueType = reader.read();
                checkEOF(valueType, "Reading value type");
                String key = readString(reader);
                Object value = readValueByType(reader, valueType);
                Cache.getInstance().put(key, value.toString(), expiryMillis);
                LoggingService.logInfo("Setting expiry for key: " + key + " value: " + value + " expiry: " + expiryMillis);
            } else if (opCode == OP_END) {
                LoggingService.logInfo("End of file reached");
                break;
            } else {
                // This for a normal key value pair
                checkEOF(opCode, "Reading value type");
                String key = readString(reader);
                Object value = readValueByType(reader, opCode);
                Cache.getInstance().put(key, value.toString(), 0);
                LoggingService.logInfo("Key: " + key + " value: " + value);
            }
        }
    }

    private Object readValueByType(DataInputStream reader, int valueType) throws IOException {
        if (valueType == VALUE_TYPE_STRING) {
            return readString(reader);
        } else if (valueType == VALUE_TYPE_LIST) {
            return readList(reader);
        } else if (valueType == VALUE_TYPE_SET) {
            return readSet(reader);
        }
        throw new IOException("Unexpected value type: " + valueType);
    }

    private Object readSet(DataInputStream reader) throws IOException {
        int size = readLengthEncodedNum(reader);
        Set<String> set = new HashSet<>(size);
        for (int i = 0; i < size; i++) {
            set.add(readString(reader));
        }
        return set;
    }

    private Object readList(DataInputStream reader) throws IOException {
        int size = readLengthEncodedNum(reader);
        List<String> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            list.add(readString(reader));
        }
        return list;
    }

    private void readResizeDBData(DataInputStream reader) throws IOException {
        int dbSize = getLengthAndEncoding(reader).length;
        int expireDBSize = getLengthAndEncoding(reader).length;
        LoggingService.logInfo("Resizing DB: " + dbSize + " expire: " + expireDBSize);
        Cache.getInstance(dbSize, expireDBSize);
    }

    private int readLengthEncodedNum(DataInputStream reader) throws IOException {
        LengthEncoding lengthEncoding = getLengthAndEncoding(reader);
        if (!lengthEncoding.encoded) {
            throw new IOException("Unexpected non-encoded length for selectdb");
        }
        int num;
        if (lengthEncoding.length == ENC_8BIT_INT) {
            num = readByte(reader);
        } else if (lengthEncoding.length == ENC_16BIT_INT) {
            num = readShort(reader);
        } else if (lengthEncoding.length == ENC_32BIT_INT) {
            num = readInt(reader);
        } else {
            throw new IOException("Unexpected encoded length: " + lengthEncoding.length);
        }
        return num;
    }

    private String readString(DataInputStream reader) throws IOException {
        LengthEncoding lengthEncoding = getLengthAndEncoding(reader);
        if (lengthEncoding.encoded) {
            if (lengthEncoding.length == ENC_8BIT_INT) {
                return String.valueOf(readByte(reader));
            } else if (lengthEncoding.length == ENC_16BIT_INT) {
                return String.valueOf(readShort(reader));
            } else if (lengthEncoding.length == ENC_32BIT_INT) {
                return String.valueOf(readInt(reader));
            } else if (lengthEncoding.length == ENC_COMPRESSED) {
                LengthEncoding compressedLenEnc = getLengthAndEncoding(reader);
                LengthEncoding uncompressedLenEnc = getLengthAndEncoding(reader);
                byte[] compressedBytes = new byte[compressedLenEnc.length];
                int read = reader.read(compressedBytes, 0, compressedBytes.length);
                if (read != compressedBytes.length) {
                    throw new IOException("Unexpected EOF reading string");
                }

                byte[] uncompressedBytes = new byte[uncompressedLenEnc.length];
                int decode = LZFDecoder.safeDecoder().decode(compressedBytes, uncompressedBytes);
                if (decode != uncompressedBytes.length) {
                    throw new IOException("Unexpected decode length: " + decode);
                }
                return new String(uncompressedBytes);
            } else {
                throw new IOException("Unexpected encoded length: " + lengthEncoding.length);
            }
        } else {
            byte[] bytes = new byte[lengthEncoding.length];
            int read = reader.read(bytes, 0, bytes.length);
            if (read != bytes.length) {
                throw new IOException("Unexpected EOF reading string");
            }
            return new String(bytes);
        }
    }

    private long readLong(DataInputStream reader) throws IOException {
        byte[] intBytes = new byte[8];
        int read = reader.read(intBytes, 0, intBytes.length);
        if (read != intBytes.length) {
            throw new IOException("Unexpected EOF reading short");
        }
        return ByteBuffer.wrap(intBytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    private int readInt(DataInputStream reader) throws IOException {
        byte[] intBytes = new byte[4];
        int read = reader.read(intBytes, 0, intBytes.length);
        if (read != intBytes.length) {
            throw new IOException("Unexpected EOF reading short");
        }
        return ByteBuffer.wrap(intBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    private short readShort(DataInputStream reader) throws IOException {
        byte[] shortBytes = new byte[2];
        int read = reader.read(shortBytes, 0, shortBytes.length);
        if (read != shortBytes.length) {
            throw new IOException("Unexpected EOF reading short");
        }
        return ByteBuffer.wrap(shortBytes).order(ByteOrder.LITTLE_ENDIAN).getShort();
    }

    private byte readByte(DataInputStream reader) throws IOException {
        int read = reader.read();
        checkEOF(read, "Reading byte");
        return (byte) read;
    }

    private char readChar(DataInputStream reader) throws IOException {
        int read = reader.read();
        checkEOF(read, "Reading char");
        return (char) read;
    }

    record LengthEncoding(int length, boolean encoded) {}

    private LengthEncoding getLengthAndEncoding(DataInputStream reader) throws IOException {
        int length = reader.read();
        checkEOF(length, "Reading length");
        int type = length & NUM_ENCODING;
        if (type == NUM_6BIT) {
            return new LengthEncoding(length & 0b00111111, false);
        } else if (type == NUM_14BIT) {
            int nextByte = reader.read();
            checkEOF(nextByte, "Reading 14bit length");
            return new LengthEncoding(((length & 0b00111111) << 8) | nextByte, false);
        } else if (type == NUM_32BIT) {
            byte[] lengthBytes = new byte[4];
            int read = reader.read(lengthBytes, 0, lengthBytes.length);
            if (read != lengthBytes.length) {
                throw new IOException("Unexpected EOF reading length");
            }
            return new LengthEncoding(ByteBuffer.wrap(lengthBytes).order(ByteOrder.LITTLE_ENDIAN).getInt(), false);
        } else {
            return new LengthEncoding(length & 0b00111111, true);
        }
    }

    private void readVersionNumber(DataInputStream reader) throws IOException {
        byte[] versionBytes = new byte[4];
        int read = reader.read(versionBytes, 0, versionBytes.length);
        if (read != versionBytes.length) {
            throw new IOException("Unexpected EOF reading version number");
        }
        version = Integer.parseInt(new String(versionBytes));
        LoggingService.logInfo("Redis RDB version: " + version);
    }

    private void verifyMagicNumber(DataInputStream reader) throws IOException {
        byte[] magicNumber = new byte[5];
        int read = reader.read(magicNumber, 0, magicNumber.length);
        if (read != magicNumber.length) {
            throw new IOException("Unexpected EOF reading magic number");
        }
        if (magicNumber[0] != 'R' || magicNumber[1] != 'E' || magicNumber[2] != 'D' || magicNumber[3] != 'I' || magicNumber[4] != 'S') {
            throw new IOException("Unexpected magic number: " + ByteBuffer.wrap(magicNumber).order(ByteOrder.LITTLE_ENDIAN).asCharBuffer());
        }
    }

    private void checkEOF(int read, String message) throws IOException {
        if (read == -1) {
            throw new IOException("Unexpected EOF: " + message);
        }
    }
}
