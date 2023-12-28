import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.opencv.core.Size;
import org.opencv.imgcodecs.Imgcodecs;
import org.opencv.imgproc.Imgproc;

import java.util.Map;

public class SharpeningBolt extends BaseRichBolt {
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
        System.out.println("SharpeningBolt - FrameFileName: " + frameFileName);
        // Apply sharpening filter using OpenCV
        Mat sharpenedImage = new Mat();
        Imgproc.GaussianBlur(imageMatrix, sharpenedImage, new Size(0, 0), 3);
        Core.addWeighted(imageMatrix, 2.0, sharpenedImage, -1.0, 0, sharpenedImage);

        // Save the sharpened image to the output folder
        String outputFolder = "Sharpened-images";
        String outputImagePath = outputFolder + "/" + frameFileName;
        Imgcodecs.imwrite(outputImagePath, sharpenedImage);

        // Emit the output image path for further processing (if needed)
        System.out.println("Emitting tuple IMAGESHARPENINGBOLT: " + input.toString());
        String id = "SharpeningBolt";
        collector.emit(input, new Values(id, sharpenedImage, frameFileName));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ID" ,"frame", "frameFileName"));
    }
}
