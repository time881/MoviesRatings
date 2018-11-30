package indi.zion.Kafka;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import indi.zion.Kafka.TextFileParser.TextReader;

public class MsgProducer {
    private Properties props = new Properties();
    private String RatePath;
    private String TagPath;
    
    public MsgProducer() {
      InitProp();
    }
    
    public void InitProp() {
        try {
            InputStream inStream = TextReader.class.getResourceAsStream("ReadText.properties");
            props.load(inStream);
            inStream.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    public void prepareMsg() {
        
    }
}
