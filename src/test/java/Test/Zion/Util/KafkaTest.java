package Test.Zion.Util;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;

import org.junit.Test;

import indi.zion.Constant.SpaceUnit;
import indi.zion.InfoStream.Beans.Rate;
import indi.zion.Kafka.TextFileParser.BeansPrep;
import indi.zion.Kafka.TextFileParser.TextReader;

public class KafkaTest {

    @Test
    public void test() {
        //fail("Not yet implemented");
    }
    
    @Test
    public void ByteReader() {
        String FilePath = "D:\\Document\\DataCollection\\jankon6_10607013\\ml-data-10M\\ml-data-10M\\TestText.txt";
        BeansPrep bp = new BeansPrep<Rate>(Rate.class);
        long offset = 0;
        TextReader reader = new TextReader(FilePath, 10+SpaceUnit.M);
        long size = reader.LoadBlock();
        offset += size;
        reader.setOffset(offset);
        System.out.println("size:"+size);
        System.out.println("block end:"+reader.getBlock()[(int) size-1]);
        System.out.println("block begin:"+reader.getBlock()[0]);
        ArrayList<Rate> al = bp.ToBeans(reader.getBlock());
        Rate tmp = al.get(0);
        System.out.println(tmp.getMovieID()+ " "+tmp.getUserID() + " "+ tmp.getTimeStamp() + " " + tmp.getRate());
        tmp = al.get(al.size()-1);
        System.out.println(tmp.getMovieID()+ " "+tmp.getUserID() + " "+ tmp.getTimeStamp() + " " + tmp.getRate());
        assertEquals(size!=-1, true);
        
        size = reader.LoadBlock();
        offset += size;
        reader.setOffset(offset);
        System.out.println("size:"+size);
        al = bp.ToBeans(reader.getBlock());
        System.out.println(al.size());
        if(al.size() > 0) {
            tmp = al.get(0);
            System.out.println(tmp.getMovieID()+ " "+tmp.getUserID() + " "+ tmp.getTimeStamp() + " " + tmp.getRate());
            tmp = al.get(al.size()-1);
            System.out.println(tmp.getMovieID()+ " "+tmp.getUserID() + " "+ tmp.getTimeStamp() + " " + tmp.getRate());
        }
        assertEquals(size!=-1, true);
        
        size = reader.LoadBlock();
        offset += size;
        reader.setOffset(offset);
        System.out.println("size:"+size);
        al = bp.ToBeans(reader.getBlock());
        System.out.println(al.size());
        if(al.size() > 0) {
            tmp = al.get(0);
            System.out.println(tmp.getMovieID()+ " "+tmp.getUserID() + " "+ tmp.getTimeStamp() + " " + tmp.getRate());
            tmp = al.get(al.size()-1);
            System.out.println(tmp.getMovieID()+ " "+tmp.getUserID() + " "+ tmp.getTimeStamp() + " " + tmp.getRate());
        }
        assertEquals(size!=-1, true);
        
        size = reader.LoadBlock();
        offset += size;
        reader.setOffset(offset);
        System.out.println("size:"+size);
        al = bp.ToBeans(reader.getBlock());
        System.out.println(al.size());
        if(al.size() > 0) {
            tmp = al.get(0);
            System.out.println(tmp.getMovieID()+ " "+tmp.getUserID() + " "+ tmp.getTimeStamp() + " " + tmp.getRate());
            tmp = al.get(al.size()-1);
            System.out.println(tmp.getMovieID()+ " "+tmp.getUserID() + " "+ tmp.getTimeStamp() + " " + tmp.getRate());
        }
        assertEquals(size!=-1, true);
    }
}
