import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.opencv.core.*;
import org.opencv.imgcodecs.Imgcodecs;
import org.opencv.videoio.VideoWriter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class AggregationBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String, Mat> blurredFrames;
    private Map<String, Mat> sharpenedFrames;
    private String outputFolder;
    private List<Mat> aggregatedFrames;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.blurredFrames = new HashMap<>();
        this.sharpenedFrames = new HashMap<>();
        this.aggregatedFrames = new ArrayList<>();
        this.outputFolder = "Aggregated-images";


    }

    @Override
    public void execute(Tuple input) {
        String id = input.getStringByField("ID");
        String fileName = input.getStringByField("frameFileName");
        Mat frame = (Mat) input.getValueByField("frame");
        // Folder containing image frames
        String framesFolder = "Aggregated-images";

        if (id.equals("GaussianBolt")) {
            System.out.println("GAUSSIAN_BOLT: --------" + id);

            blurredFrames.put(fileName, frame);
        } else if (id.equals("SharpeningBolt")) {
            System.out.println("SHARPENING_BOLT: --------" + id);
            sharpenedFrames.put(fileName, frame);
        }
        System.out.println("BLURRED_FRAME_SIZE: --------" + blurredFrames.size());
        System.out.println("NOT_LOOP: --------" + blurredFrames.size());
        if (blurredFrames.size() == 902 && sharpenedFrames.size() == 902) {
            // Perform aggregation and save result
            System.out.println("IN_LOOP: --------" + blurredFrames.keySet());
            for (String key : blurredFrames.keySet()) {
                Mat blurredFrame = blurredFrames.get(key);
                Mat sharpenedFrame = sharpenedFrames.get(key);
                System.out.println("KEY: --------" + key);
                System.out.println("blurredFrame: --------" + blurredFrame);
                System.out.println("sharpenedFrame: --------" + sharpenedFrame);
                Mat aggregatedFrame = new Mat();
                Core.add(blurredFrame, sharpenedFrame, aggregatedFrame);

                // Save the aggregated frame to the "Aggregated-images" folder
                String outputPath = "Aggregated-images/" + key;
                Imgcodecs.imwrite(outputPath, aggregatedFrame);

                // Store the aggregated frame for video creation
                aggregatedFrames.add(aggregatedFrame);
            }
            System.out.println("TOTAL-FRAMES:" + blurredFrames.size());
            int totalFrames = blurredFrames.size();
            // Clear the frames for the next batch
            blurredFrames.clear();
            sharpenedFrames.clear();
            // Check if the total number of frames is 902 to create a video
            if (aggregatedFrames.size() == totalFrames) {

                // Load frames from the folder
                List<Mat> frames = loadFrames(framesFolder);
                // Calculate average brightness and save to log file
                saveAverageBrightnessToLog(frames, "aggregated-log.txt");
                System.out.println("FRAMES: --------" + frames);
                createVideo(frames);
                frames.clear();  // Clear frames after creating the video
            }
        }

        // No need to emit anything here since we're just saving the images
        collector.ack(input);
    }
    private double calculateBrightness(Mat frame) {
        Scalar scalar = Core.sumElems(frame);
        return scalar.val[0] / (frame.cols() * frame.rows());
    }
    private void saveAverageBrightnessToLog(List<Mat> frames ,String logFilePath) {
        // Save average brightness information to log file
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(logFilePath))) {
            writer.write("FrameNumber,FrameFileName,AverageBrightness\n");

            for (int i = 0; i < frames.size(); i++) {
                Mat frame = frames.get(i);
                double averageBrightness = calculateBrightness(frame);
                writer.write(i + "," + "frame_" + i + ".jpg" + "," + averageBrightness + "\n");
            }
        } catch (IOException e) {
            throw new RuntimeException("Error writing to log file.");
        }
    }


    private static List<Mat> loadFrames(String folderPath) {
        List<Mat> frames = new ArrayList<>();
        File folder = new File(folderPath);

        if (folder.exists() && folder.isDirectory()) {
            File[] files = folder.listFiles();

            if (files != null) {
                // Sort files in natural order based on their names
                List<File> sortedFiles = new ArrayList<>(List.of(files));
                Collections.sort(sortedFiles, Comparator.comparing(File::getName, new NumericStringComparator()));

                for (File file : sortedFiles) {
                    System.out.println("FILES: --------" + file.getName());
                    if (file.isFile() && isImageFile(file.getName())) {
                        Mat frame = Imgcodecs.imread(file.getAbsolutePath());
                        if (!frame.empty()) {
                            frames.add(frame);
                        }
                    }
                }
            }
        }

        return frames;
    }
    private static boolean isImageFile(String fileName) {
        return fileName.toLowerCase().endsWith(".jpg") ||
                fileName.toLowerCase().endsWith(".jpeg") ||
                fileName.toLowerCase().endsWith(".png") ||
                fileName.toLowerCase().endsWith(".gif") ||
                fileName.toLowerCase().endsWith(".bmp");
    }

    private static class NumericStringComparator implements Comparator<String> {
        @Override
        public int compare(String s1, String s2) {
            // Extract numeric parts and compare as integers
            int n1 = extractNumber(s1);
            int n2 = extractNumber(s2);
            return Integer.compare(n1, n2);
        }

        private int extractNumber(String s) {
            // Extract numeric part from the string
            String[] parts = s.split("\\D+");
            if (parts.length > 0) {
                return Integer.parseInt(parts[parts.length - 1]);
            }
            return 0;
        }
    }

    private void createVideo(List<Mat> frames) {
        String outputVideoPath = "Aggregated-video/output_video.mp4";
        int fps = 30; // Frames per second
        Size frameSize = new Size(frames.get(0).cols(), frames.get(0).rows());

        VideoWriter videoWriter = new VideoWriter(outputVideoPath, VideoWriter.fourcc('m', 'p', '4', 'v'), fps, frameSize);

        if (!videoWriter.isOpened()) {
            System.out.println("Error: VideoWriter not opened");
            return;
        }

        for (Mat frame : frames) {
            videoWriter.write(frame);
        }

        videoWriter.release();
        System.out.println("Video created successfully.");
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
