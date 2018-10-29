package cn.itcast.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import org.apache.commons.lang.StringUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MyLocalFileSpout extends BaseRichSpout {

    private SpoutOutputCollector spoutOutputCollector;
    private BufferedReader bufferedReader ;

    /**
     * 初始化方法（被循环调用的方法）
     * @param map
     * @param topologyContext
     * @param spoutOutputCollector
     */
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        try {
            this.bufferedReader = new BufferedReader( new FileReader( new File("D:/bigDataJob/wordcount/input/1.txt") ));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }

    /**
     * Storm实时计算的特性就是对数据一条一条的处理
     * while(true){ this.nextTuple(); }
     */
    @Override
    public void nextTuple() {
        try {
            String line = bufferedReader.readLine();
            if(StringUtils.isNotBlank( line )){
                //每调用一次就会发送一次数据
                List<Object> list = new ArrayList<Object>();
                list.add( line );
                spoutOutputCollector.emit( list );
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //定义发送的数据是什么
        outputFieldsDeclarer.declare( new Fields( "juzi" ));
    }
}
