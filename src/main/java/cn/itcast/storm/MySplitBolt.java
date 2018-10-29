package cn.itcast.storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class MySplitBolt extends BaseBasicBolt {


    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        // 数据如何获取
        String juzi = (String) tuple.getValueByField("juzi");
        // 进行切割
        String[] strings = juzi.split( " " );
        // 发送数据
        for( String word : strings ){
            // Values对象帮我们自动生成list
            basicOutputCollector.emit( new Values(word,1));
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare( new Fields( "word","num" ));
    }
}
