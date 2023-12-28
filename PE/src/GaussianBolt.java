import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import org.opencv.core.Mat;
import org.opencv.core.Size;
import org.opencv.imgcodecs.Imgcodecs;
import org.opencv.imgproc.Imgproc;

import java.util.Map;

public class GaussianBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        // Get the image matrix and frame filename from the tuple


        Mat imageMatrix = (Mat) input.getValueByField("frameMatrix");
        String frameFileName = input.getStringByField("frameFileName");
        System.out.println("GaussianBolt - FrameFileName: " + frameFileName);
        // Apply GaussianBlur filter using OpenCV
        Mat blurredImage = new Mat();
        Imgproc.GaussianBlur(imageMatrix, blurredImage, new Size(0, 0), 3);

        // Save the blurred image to the output folder
        String outputFolder = "Gaussian-images";
        String outputImagePath = outputFolder + "/" + frameFileName;
        Imgcodecs.imwrite(outputImagePath, blurredImage);
//        System.out.println("Blurred Matrix for frame " + frameFileName + ":\n" + blurredImage.dump());
        // Emit the output image path for further processing (if needed)
        System.out.println("Emitting tuple IMAGEPROCESSINGBOLT: " + input.toString());
        String id = "GaussianBolt";
        collector.emit(input, new Values(id ,blurredImage, frameFileName));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ID", "frame", "frameFileName"));
    }
}
