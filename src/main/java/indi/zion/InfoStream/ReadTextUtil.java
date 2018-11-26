package indi.zion.InfoStream;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import org.apache.spark.sql.catalyst.expressions.Length;

import indi.zion.Constant.SpaceUnit;
import indi.zion.Constant.StrSplitSign;

public class ReadTextUtil {

    private String TextPath;
    private String BlockCapcity;
    private long Offset;
    private byte[] Block;
    
    public ReadTextUtil(String TextPath) {
        this(TextPath, 1+SpaceUnit.M);
    }
    
    public ReadTextUtil(String TextPath, String BlockCapcity) {
        this(TextPath, BlockCapcity, 0);
    }
    
    public ReadTextUtil(String TextPath, String BlockCapcity, int Offset) {
        // TODO Auto-generated constructor stub
        this.TextPath = TextPath;
        this.Offset = Offset;
        this.BlockCapcity = BlockCapcity;
    }

    public void setTextPath(String textPath) {
        this.TextPath = textPath;
        this.ClearBlock();
    }
    
    public void ClearBlock() {
        this.Block = null;
    }
    
    //change B/K/M/G etc to B
    public Double FormatUnit(String Num) {
        int len = Num.length();
        for(int i=len-1; i>=0; i--) {
            if(Num.charAt(i)>=48 &&Num.charAt(i)<=57) {
                String Unit = Num.substring(i+1);
                double value = Double.valueOf(Num.substring(0, i+1));
                if(SpaceUnit.K.equals(Unit)) {
                    return value*1024.0;
                }else if(SpaceUnit.M.equals(Unit)){
                    return value*1024.0*1024.0;
                }else if(SpaceUnit.B.equals(Unit)){
                    return value;
                }else if(SpaceUnit.G.equals(Unit)){
                    return value*1024.0*1024.0*1024.0;
                }
            }
        }
        return 0.0;
    }
    
  //Cut by the end of /n/r characters and return after read size
    public int Trim(byte[] bytes) {
        for(int i = bytes.length-1; i >= 0; i--) {
            if(bytes[i] == StrSplitSign.n || bytes[i] == StrSplitSign.r) {
                return i;
            }else {
                bytes[i] = 0;
            }
        }
        return 0;
    }
    
    public int GetBlock() {
        try {
            File file = new File(TextPath);
            InputStream inputStream = new FileInputStream(file);
            inputStream.skip(Offset-1);
            InputStream sbs = new BufferedInputStream(inputStream);
            ByteArrayOutputStream readedByte = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            double blockSize = FormatUnit(BlockCapcity);
            int readedSize = 0;
            int readLength = 0;
            while((readLength = sbs.read(buffer)) != -1 && readedSize < blockSize) {
                readedSize += readLength;
                readedByte.write(buffer, 0, readLength);
            }
            Block = readedByte.toByteArray();
            readedSize = Trim(Block);
            inputStream.close();
            sbs.close();
            return readedSize;
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return -1;
    }
}
