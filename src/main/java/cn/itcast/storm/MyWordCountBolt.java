package cn.itcast.storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class MyWordCountBolt extends BaseBasicBolt {

    Map<String,Integer> map = new HashMap<String,Integer>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        String word = (String) tuple.getValueByField( "word" );
        Integer num = (Integer) tuple.getValueByField( "num" );

        Integer integer = map.get( word );
        if(  integer==null || integer.intValue()==0 ){
            map.put( word,num );
        }else{
            map.put( word ,integer.intValue()+num  );
        }
        System.out.println( map );

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // 不需要定义输出字段
    }
}
