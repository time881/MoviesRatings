package indi.zion.InfoStream.Beans;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

public class BeanEncoder<T extends Bean> implements Serializer<T>{

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // TODO Auto-generated method stub
    }
    
    @Override
    public byte[] serialize(String topic, T data) {
        // TODO Auto-generated method stub
        return data.toBytes();
    }
    
    @Override
    public void close() {
        // TODO Auto-generated method stub
        
    }
}
