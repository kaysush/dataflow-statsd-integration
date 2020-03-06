import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface CustomOptions extends PipelineOptions {

    @Description("Port at which StatsD server is running")
    @Default.Integer(9125)
    int getStatsDPort();
    void setStatsDPort(int port);


    @Description("Host at which StatsD server is running")
    @Default.String("localhost")
    String getStatsDHost();
    void setStatsDHost(String host);

    @Description("Path to write valid JSON events.")
    @Default.String("")
    String getOutputPath();
    void setOutputPath(String outputPath);

}
