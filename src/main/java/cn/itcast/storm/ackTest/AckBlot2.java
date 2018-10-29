package cn.itcast.storm.ackTest;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

public class AckBlot2 extends BaseRichBolt {

    OutputCollector outputCollector;

    //初始化方法之调用头一次
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    //循环调用
    @Override
    public void execute(Tuple tuple) {
        outputCollector.emit( tuple,new Values( tuple.getString( 0 ) ));

        System.out.println( "blot2的execute被调用一次" + tuple.getString( 0 ));

        outputCollector.ack( tuple );
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare( new Fields( "uuid" ));
    }

}
