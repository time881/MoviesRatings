package indi.zion.Kafka.TextFileParser;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import org.apache.spark.sql.catalyst.expressions.Length;

import indi.zion.Constant.SpaceUnit;
import indi.zion.Constant.StrSplitSign;
import indi.zion.Util.CommonUtil;

/**
 * getting given size of Block, Trim after last "\r|\n"
 * 
 * @author Administrator
 *
 */
public class TextReader {
    private String TextPath;
    private double BlockCapcity;
    private long Offset;
    private byte[] Block;

    public TextReader(String TextPath) {
        this(TextPath, 0.5 + SpaceUnit.M);
    }

    public TextReader(String TextPath, String BlockCapcity) {
        this(TextPath, BlockCapcity, 0);
    }

    public TextReader(String TextPath, String BlockCapcity, int Offset) {
        // TODO Auto-generated constructor stub
        this.TextPath = TextPath;
        this.Offset = Offset;
        this.BlockCapcity = CommonUtil.FormatUnit(BlockCapcity);
    }

    public void setOffset(long offset) {
        Offset = offset;
    }

    public long getOffset() {
        return Offset;
    }

    public void setTextPath(String textPath) {
        this.TextPath = textPath;
        this.setOffset(0);
        this.clearBlock();
    }

    public byte[] getBlock() {
        return Block;
    }

    public void clearBlock() {
        this.Block = null;
    }

    public int LoadBlock() {
        try {
            File file = new File(TextPath);
            InputStream inputStream = new FileInputStream(file);
            inputStream.skip(Offset - 1);
            InputStream sbs = new BufferedInputStream(inputStream);
            ByteArrayOutputStream readedByte = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            double blockSize = BlockCapcity;
            int readSize = 0;
            int readLength = 0;
            while ((readLength = sbs.read(buffer)) != -1 && readSize < blockSize) {
                readSize += readLength;
                readedByte.write(buffer, 0, readLength);
            }
            Block = readedByte.toByteArray();
            readSize = Trim(Block);
            inputStream.close();
            sbs.close();
            return readSize;
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return -1;
    }

    // Cut by the end of /n|/r characters and return after read size
    private int Trim(byte[] bytes) {
        for (int i = bytes.length - 1; i >= 0; i--) {
            if (bytes[i] == StrSplitSign.n || bytes[i] == StrSplitSign.r) {
                return i;
            } else {
                bytes[i] = 0;
            }
        }
        return 0;
    }
}
