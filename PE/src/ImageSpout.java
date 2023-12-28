import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.opencv.core.Scalar;
import org.opencv.core.Size;
import org.opencv.imgcodecs.Imgcodecs;
import org.opencv.imgproc.Imgproc;
import org.opencv.videoio.VideoCapture;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class ImageSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private String videoFilePath;
    private VideoCapture videoCapture;
    private int totalFrames;
    private int currentFrameNumber;
    private String framesOutputFolder;
    private String logFilePath;

    public ImageSpout(String videoFilePath, String framesOutputFolder, String logFilePath) {
        this.videoFilePath = videoFilePath;
        this.framesOutputFolder = framesOutputFolder;
        this.logFilePath = logFilePath;
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.videoCapture = new VideoCapture(videoFilePath);
        if (!videoCapture.isOpened()) {
            throw new RuntimeException("Error opening video file.");
        }
        // Calculate the total number of frames
        this.totalFrames = (int) videoCapture.get(7); // 7 corresponds to CV_CAP_PROP_FRAME_COUNT

        this.currentFrameNumber = 0;

        // Create frames output folder if it doesn't exist
        File outputFolder = new File(framesOutputFolder);
        if (!outputFolder.exists()) {
            if (!outputFolder.mkdirs()) {
                throw new RuntimeException("Error creating frames output folder.");
            }
        }

        // Create or clear the log file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(logFilePath))) {
            // Write header if the file is newly created
            writer.write("FrameNumber,FrameFileName,AverageBrightness\n");
        } catch (IOException e) {
            throw new RuntimeException("Error creating or clearing log file.");
        }
    }

    @Override
    public void nextTuple() {
        if (currentFrameNumber < totalFrames) {
            Mat frame = new Mat();
            videoCapture.read(frame);

            if (!frame.empty()) {
                // Step 3: Image Processing Operations
                Mat grayscaleFrame = new Mat();
                Imgproc.cvtColor(frame, grayscaleFrame, Imgproc.COLOR_BGR2GRAY);

                // Resize frames to a predetermined smaller size
                Size newSize = new Size(320, 240);
                Imgproc.resize(grayscaleFrame, grayscaleFrame, newSize);

                // Save the processed frame to the frames output folder
                String frameFileName = "frame_" + currentFrameNumber + ".jpg";
                String outputPath = framesOutputFolder + File.separator + frameFileName;
                Imgcodecs.imwrite(outputPath, grayscaleFrame);

                // Calculate average brightness
                double averageBrightness = calculateBrightness(grayscaleFrame);

                // Save frame information to log file
                saveToLogFile(currentFrameNumber, frameFileName, averageBrightness);

                // Step 4: Emit the frame matrix and frame file name
                collector.emit(new Values(grayscaleFrame, frameFileName));

                // Increment the current frame number
                currentFrameNumber++;
            }
        }
    }

    private double calculateBrightness(Mat frame) {
        Scalar scalar = Core.sumElems(frame);
        return scalar.val[0] / (frame.cols() * frame.rows());
    }

    private void saveToLogFile(int frameNumber, String frameFileName, double averageBrightness) {
        // Save frame information to log file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(logFilePath, true))) {
            writer.write(frameNumber + "," + frameFileName + "," + averageBrightness + "\n");
        } catch (IOException e) {
            throw new RuntimeException("Error writing to log file.");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("frameMatrix", "frameFileName"));
    }

    @Override
    public void close() {
        videoCapture.release();
    }
}
