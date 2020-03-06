import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;

import java.util.Arrays;
import java.util.List;

public class App {
    public static void main(String[] args) {
        CustomOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(CustomOptions.class);
        String statsDHort = options.getStatsDHost();
        int statsDPort = options.getStatsDPort();
        String outputPath = options.getOutputPath();

        Pipeline p = Pipeline.create(options);

        final List<String> DATA = Arrays.asList(
                "{ \"name\": \"John\", \"age\": \"20\" }",
                "{ \"name\": \"John\" }",
                "{ \"name\": \"John\", \"age\": \"20\", \"tel\": \"0000000000\" }"
        );

        ValidateJson jsonValidator = new ValidateJson()
                .setStatsDHost(statsDHort)
                .setStatsDPort(statsDPort)
                .setValidKeys(Arrays.asList("name", "age"))
                .setMandatoryKeys(Arrays.asList("name", "age"));

        p.apply(Create.of(DATA)).setCoder(StringUtf8Coder.of())
                .apply("Validate Messages", ParDo.of(jsonValidator))
        .apply("Write Valid Message", TextIO.write().to(outputPath));

        p.run();
    }
}
