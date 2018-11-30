package indi.zion.Kafka;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import indi.zion.InfoStream.Beans.Bean;
import indi.zion.Kafka.TextFileParser.BeansPrep;
import indi.zion.Kafka.TextFileParser.TextReader;
import indi.zion.Util.CommonUtil;

public class ReadController<T extends Bean> {
    private double requestedSize;
    private TextReader byteReader;
    private BeansPrep beansPrep;
    private Class<?> beanClass;
    private int offset = 0;

    public ReadController(String filePath, String requestedSize, Class<T> beanClass) {
        this.beanClass = beanClass;
        this.byteReader = new TextReader(filePath);
        this.requestedSize = CommonUtil.FormatUnit(requestedSize);
        this.beansPrep = new BeansPrep<T>(beanClass);
    }

    public ArrayList<T> Read() {
        int readSize = -1;
        //init pool
        ThreadPoolExecutor executor = new ThreadPoolExecutor(4, 4, 1, TimeUnit.DAYS, 
                new LinkedBlockingQueue<Runnable>(8),
                new RejectedExecutionHandler() {
                    @Override
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        if (!executor.isShutdown()) {
                            try {
                                executor.getQueue().put(r);//to be waiting when cannot insert into queue
                            } catch (InterruptedException e) {
                                // should not be interrupted
                            }
                        }
                    }
                });
        Object syn = new Object();
        ArrayList<T> beans = new ArrayList<T>();
        while (requestedSize > byteReader.getOffset() || readSize != 0) {
            readSize = byteReader.LoadBlock();
            offset += readSize;
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    // TODO Auto-generated method stub
                    ArrayList<T> Temp = beansPrep.ToBeans(byteReader.getBlock());
                    synchronized (syn) {
                        beans.addAll(Temp);
                    }
                }
            });
            byteReader.setOffset(offset);
        }
        return beans;
    }
}
