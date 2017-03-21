
import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.EndpointInject;
import org.apache.camel.ProducerTemplate;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Before;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;

import se.kth.infosys.camel.ug.internal.UgMessage;

public abstract class UgCamelTestBase extends AbstractJUnit4SpringContextTests {
    protected static final JSONParser parser = new JSONParser(); 

    @Autowired
    protected CamelContext camelContext;

    @EndpointInject(uri = "direct:start")
    protected ProducerTemplate producer;    

    static JSONArray readJsonFromFile(String fileName) throws Exception {
        URL file = UgCamelTestBase.class.getClassLoader().getResource(fileName);
        return (JSONArray) parser.parse(new FileReader(new File(file.toURI())));
    }

    private JSONArray data;

    @Before
    public void sendMessages() throws Exception {
        data = readJsonFromFile(this.getClass().getName() + ".json");
        int messageId = 0;

        Map<String, Object> headers = new HashMap<>();
        headers.put("JMSMessageID", String.valueOf(++messageId));
        headers.put(UgMessage.Header.SyncType, UgMessage.SyncType.Incremental);
        headers.put(UgMessage.Header.Operation, UgMessage.Operation.SyncStart);
        headers.put(UgMessage.Header.Version, 1);
        producer.sendBodyAndHeaders(null, headers);

        for (int i = 0; i < data.size(); i++) {
            JSONObject jsonObject = (JSONObject) data.get(i);

            headers = new HashMap<>();
            headers.put("JMSMessageID", String.valueOf(++messageId));
            headers.put(UgMessage.Header.SyncType, UgMessage.SyncType.Incremental);
            headers.put(UgMessage.Header.Operation, UgMessage.Operation.Update);
            headers.put(UgMessage.Header.Class, jsonObject.get("ugClass"));
            headers.put(UgMessage.Header.Kthid, jsonObject.get("kthid"));
            headers.put(UgMessage.Header.Version, 1);
            producer.sendBodyAndHeaders(jsonObject.toJSONString().getBytes(), headers);
        }

        headers = new HashMap<>();
        headers.put("JMSMessageID", String.valueOf(++messageId));
        headers.put(UgMessage.Header.SyncType, UgMessage.SyncType.Incremental);
        headers.put(UgMessage.Header.Operation, UgMessage.Operation.SyncDone);
        headers.put(UgMessage.Header.Version, 1);
        producer.sendBodyAndHeaders(null, headers);
    }
}
