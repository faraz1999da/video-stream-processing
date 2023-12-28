import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class ImageProcessingTopology {
    static {
        // Load the OpenCV native library
        System.load("C:/Users/Faraz/Downloads/opencv/build/java/x64/opencv_java480.dll"); // For Windows
        // System.load("path/to/libopencv_java<version>.so"); // For Linux
    }
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = createTopology();

        // Configuration
        Config config = new Config();
        config.setDebug(true);

        // Local cluster for testing
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("image-processing-topology", config, builder.createTopology());

        // Wait for some time and then kill the cluster
        Utils.sleep(100000);
        cluster.killTopology("image-processing-topology");
        cluster.shutdown();
    }

    private static TopologyBuilder createTopology() {
        TopologyBuilder builder = new TopologyBuilder();

        // Spout configuration
        String framesOutputFolder = "frames";
        String videoFilePath = "Input-video/Wildlife.mp4";
        String logFilePath = "log.txt";
        builder.setSpout("imageSpout", new ImageSpout(videoFilePath, framesOutputFolder,logFilePath), 1);

        // Bolt configuration
        builder.setBolt("gaussianBolt", new GaussianBolt(), 2)
                .shuffleGrouping("imageSpout");

        // Image sharpening bolt
        builder.setBolt("sharpeningBolt", new SharpeningBolt(), 2)
                .shuffleGrouping("imageSpout");

        builder.setBolt("aggregationBolt", new AggregationBolt(), 1)
                .shuffleGrouping("gaussianBolt")
                .shuffleGrouping("sharpeningBolt");


        return builder;
    }
}
