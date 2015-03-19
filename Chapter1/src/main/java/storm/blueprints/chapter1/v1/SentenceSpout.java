package storm.blueprints.chapter1.v1;

import java.util.Map;

import storm.blueprints.utils.Utils;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class SentenceSpout extends BaseRichSpout {

    private static final long serialVersionUID = 7955016999285265225L;

    private SpoutOutputCollector collector;

    private String[] sentences = { "my dog has fleas", "i like cold beverages", "the dog ate my homework",
            "don't have a cow man", "i don't think i like fleas" };

    private int index = 0;

    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

    public void open(final Map config, final TopologyContext context, final SpoutOutputCollector collector) {
        this.collector = collector;
    }

    public void nextTuple() {
        this.collector.emit(new Values(sentences[index]));
        index++;
        if (index >= sentences.length) {
            index = 0;
        }
        Utils.waitForMillis(1);
    }

}
