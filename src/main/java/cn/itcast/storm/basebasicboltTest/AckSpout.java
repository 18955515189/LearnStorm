package cn.itcast.storm.basebasicboltTest;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.UUID;

public class AckSpout extends BaseRichSpout {
    SpoutOutputCollector spoutOutputCollector;
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector=spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        String uuid = UUID.randomUUID().toString().replace("_","");
        spoutOutputCollector.emit( new Values( uuid ),uuid);
        try {
            Thread.sleep( 10*1000 );
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer OutputFieldsDeclarer) {
        OutputFieldsDeclarer.declare(new Fields( "uuid" ));
    }

    @Override
    public void ack(Object msgId) {
        System.out.println( "消息处理成功："+msgId.toString() );
    }

    @Override
    public void fail(Object msgId) {
        System.out.println( "消息处理失败："+msgId.toString() );
        spoutOutputCollector.emit( new Values( msgId.toString() ),msgId.toString());

    }
}
