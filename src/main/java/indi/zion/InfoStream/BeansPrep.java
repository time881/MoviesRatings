package indi.zion.InfoStream;

import java.util.ArrayList;

import indi.zion.Constant.StrSplitSign;

/**
 * functions about transform the data to Message as simulating Action of Kafka
 * producer thread of transform and String to beans
 */
public class MsgPrep<T> {
    public ArrayList<T> wares;

    public void MsgPrep(byte[] Block) {
        int beanStart = 0;
        int beanEnd = 0;
        int no = 0;//to sign the value No. in obj
        for (int i = 0; i < Block.length; i++) {
            if (Block[i] == StrSplitSign.ls && Block[i + 1] == StrSplitSign.ls) {
                beanEnd = i;
                no += 1;
        
                beanStart = i;
            }
            if (Block[i] == StrSplitSign.n || Block[i] == StrSplitSign.r) {
                beanEnd = i;

                beanStart = i;
                no = 0;
            }
        }
    }
}
