package indi.zion.Kafka.TextFileParser;

public interface BlockReader {

    public int getTransableSize(byte[] bytes);

    public byte[] LoadBlock(long Offset);
}
