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
public class TextReader implements BlockReader{
    private String TextPath;
    private File file;
    private double BlockCapcity;
    private static String unitSpeed = 0.05 + SpaceUnit.K;

    public TextReader(String TextPath) {
        this(TextPath, unitSpeed);
    }

    public TextReader(String TextPath, String BlockCapcity) {
        this(TextPath, BlockCapcity, 0);
    }

    public TextReader(String TextPath, String BlockCapcity, int Offset) {
        // TODO Auto-generated constructor stub
        this.TextPath = TextPath;
        this.file = new File(this.TextPath);
        this.BlockCapcity = CommonUtil.FormatUnit(BlockCapcity);
    }

    public void resetTextPath(String textPath) {
        this.TextPath = textPath;
    }

    @Override
    public byte[] LoadBlock(long Offset) {
        try {
            InputStream inputStream = new FileInputStream(file);
            inputStream.skip(Offset);
            InputStream sbs = new BufferedInputStream(inputStream);
            ByteArrayOutputStream readedByte = new ByteArrayOutputStream();
            Double bufferSize = CommonUtil.FormatUnit(unitSpeed);
            byte[] buffer = new byte[bufferSize.intValue()];
            double blockSize = BlockCapcity;
            int readSize = 0;
            int readLength = 0;
            while ((readLength = sbs.read(buffer)) != -1 && readSize < blockSize) {
                readSize += readLength;
                readedByte.write(buffer, 0, readLength);
            }
            inputStream.close();
            sbs.close();
            return readedByte.toByteArray();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    // Cut by the end of /n|/r characters and return after read size
    public int getTransableSize(byte[] bytes) {
        for (int i = bytes.length - 1; i >= 0; i--) {
            if (bytes[i] == StrSplitSign.n || bytes[i] == StrSplitSign.r || bytes[i] == StrSplitSign.end) {
                return i;
            } else {
                bytes[i] = 0;
            }
        }
        return 0;
    }
}
