package indi.zion.InfoStream.Beans;

import kafka.serializer.Decoder;

public class BeanDecoder<T extends Bean> implements Decoder<T> {

    @SuppressWarnings("unchecked")
    @Override
    public T fromBytes(byte[] arg0) {
        // TODO Auto-generated method stub
        return (T) T.toBean(arg0);
    }
}
