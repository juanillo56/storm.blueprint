package storm.blueprints.chapter1.v1;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitSentenceBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1106589616886106818L;

    private OutputCollector collector;

    public void prepare(final Map config, final TopologyContext context, final OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(final Tuple tuple) {
        String sentence = tuple.getStringByField("sentence");
        String[] words = sentence.split(" ");
        for (String word : words) {
            this.collector.emit(new Values(word));
        }
    }

    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
