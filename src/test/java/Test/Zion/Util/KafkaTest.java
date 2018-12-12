package Test.Zion.Util;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;

import org.junit.Test;

import indi.zion.Constant.SpaceUnit;
import indi.zion.InfoStream.Beans.Rate;
import indi.zion.Kafka.MsgConsumer;
import indi.zion.Kafka.MsgProducer;
import indi.zion.Kafka.ReadController;
import indi.zion.Kafka.TextFileParser.BeansPrep;
import indi.zion.Kafka.TextFileParser.TextReader;

public class KafkaTest {

    @Test
    public void test() {
        //fail("Not yet implemented");
    }
    
    //@Test
    public void ByteReadAndTransferTest() {
        String FilePath = "D:\\Document\\DataCollection\\jankon6_10607013\\ml-data-10M\\ml-data-10M\\TestText.txt";
        BeansPrep bp = new BeansPrep<Rate>(Rate.class);
        long offset = 0;
        TextReader reader = new TextReader(FilePath, 0.2+SpaceUnit.M);
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
        System.out.println("ByteReadAndTransferTest pass-----------------------------------------------------------");
    }
    
    //@Test
    public void ReadController() {
        String FilePath = "D:\\Document\\DataCollection\\jankon6_10607013\\ml-data-10M\\ml-data-10M\\TestText.txt";
        ReadController rc = new ReadController<Rate>(FilePath, 1+SpaceUnit.M, Rate.class, 0);
        ArrayList<Rate> list = rc.Read();
        Rate tmp = list.get(list.size()-1);
        System.out.println(list.size());
        System.out.println(tmp.getMovieID()+ " "+tmp.getUserID() + " "+ tmp.getTimeStamp() + " " + tmp.getRate());
    }
    
    //@Test
    public void kafkaProcedure() {
        MsgProducer mp = new MsgProducer();
        mp.SendMsg();
    }
    @Test
    public void KafkaConsume() {
        MsgConsumer mc = new MsgConsumer();
        mc.Consumer();
    }
    
}
