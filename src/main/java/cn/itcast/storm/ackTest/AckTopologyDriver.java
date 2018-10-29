package cn.itcast.storm.ackTest;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import cn.itcast.storm.MyLocalFileSpout;
import cn.itcast.storm.MySplitBolt;
import cn.itcast.storm.MyWordCountBolt;

/**
 * Created by maoxiangyi on 2016/8/16.
 */
public class AckTopologyDriver {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        //1、准备任务信息
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("mySpout", new AckSpout());
        topologyBuilder.setBolt("bolt1", new AckBlot1()).shuffleGrouping("mySpout");
        topologyBuilder.setBolt("bolt2", new AckBlot2()).shuffleGrouping("bolt1");
        topologyBuilder.setBolt("bolt3", new AckBlot3()).shuffleGrouping("bolt2");
        topologyBuilder.setBolt("bolt4", new AckBlot4()).shuffleGrouping("bolt3");

        //2、任务提交
        //提交给谁？提交什么内容？
        Config config = new Config();
        //config.setNumWorkers(2);
        StormTopology stormTopology = topologyBuilder.createTopology();
//        本地模式
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("wordcount", config, stormTopology);
        //集群模式
        //StormSubmitter.submitTopology("wordcount1", config, stormTopology);
    }
}
