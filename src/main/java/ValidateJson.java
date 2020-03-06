import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import org.apache.beam.sdk.transforms.DoFn;

import java.util.List;
import java.util.stream.Collectors;

public class ValidateJson extends DoFn<String, String> {

    private StatsDClient Statsd = null;
    private List<String> validKeys = null;
    private List<String> mandatoryKeys = null;
    private String statsDHost = "";
    private int statsDPort = 9125;

    public ValidateJson setValidKeys(List<String> validKeys){
        this.validKeys = validKeys;
        return this;
    }

    public ValidateJson setMandatoryKeys(List<String> mandatoryKeys){
        this.mandatoryKeys = mandatoryKeys;
        return this;
    }

    public ValidateJson setStatsDHost(String statsDHost){
        this.statsDHost = statsDHost;
        return this;
    }

    public ValidateJson setStatsDPort(int statsDPort){
        this.statsDPort = statsDPort;
        return this;
    }



    @Setup
    public void startup(){
        this.Statsd = new NonBlockingStatsDClient("statsd_dataflow", statsDHost, statsDPort);
    }


    @ProcessElement
    public void processElement(ProcessContext context){
        String jsonStr = context.element();
        JsonObject json = new JsonParser().parse(jsonStr).getAsJsonObject();
        if(!hasMandatoryFields(json)){
            this.Statsd.incrementCounter("rejected.mandatory_check.reason.missing_mandatory");
            return; // Reject the message
        }

        if(!hasValidKeys(json)){
            this.Statsd.incrementCounter("rejected.valid_check.reason.extra_keys");
            return; // Reject the message
        }

        context.output(jsonStr);
    }

    @Teardown
    public void teardown(){
        this.Statsd.close();
    }

    private List<String> getJsonFields(JsonObject json){
        return json.entrySet().stream().map(entry -> entry.getKey()).collect(Collectors.toList());

    }

    private boolean hasMandatoryFields(JsonObject json){
        List<String> fields = getJsonFields(json);
        for(String key : this.mandatoryKeys){
            if(!fields.contains(key)){
                return false;
            }
        }

        return true;
    }

    private boolean hasValidKeys(JsonObject json){
        List<String> fields = getJsonFields(json);
        for(String field: fields){
            if(!this.validKeys.contains(field)){
                return false;
            }
        }
        return true;
    }


}
