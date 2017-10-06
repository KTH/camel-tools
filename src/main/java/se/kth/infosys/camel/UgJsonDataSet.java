/*
 * MIT License
 *
 * Copyright (c) 2017 Kungliga Tekniska högskolan
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package se.kth.infosys.camel;

import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.camel.Exchange;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import se.kth.infosys.camel.ug.UgMessage;

/**
 * @deprecated Use version in maven artifact camel-ug-test-tools instead.
 * 
 * A DataSet that reads JSON encoded UG payloads from a file.
 * 
 * The file is assumed to contain a stream of objects used as part of one UG sync
 * event. UgMessage start and done messages with empty bodies will be inserted
 * before and after this stream.
 * 
 * Relevant UgMessage headers will be added to messages produced by this
 * dataset.
 * 
 * Example of use:
 * <pre>
 * &lt;bean id="ugDataSet" class="se.kth.infosys.camel.UgJsonDataSet"&gt;
 *   &lt;property name="sourceFile" value="classpath:ug-data.json"/&gt;
 *   &lt;property name="size" value="32"/&gt;
 * &lt;/bean&gt;
 * ...
 * &lt;from uri="dataset:ugDataSet" /&gt;
 * </pre>
 */
@Deprecated
public class UgJsonDataSet extends JsonDataSet {
    protected static final JSONParser parser = new JSONParser();
    
    public UgJsonDataSet() {}

    /**
     * Constructor taking the name of a source file.
     * @param sourceFileName the name of the file.
     * @throws Exception on file access and parse errors.
     */
    public UgJsonDataSet(String sourceFileName) throws Exception {
        super(sourceFileName);
    }

    /**
     * Constructor taking a File object.
     * @param sourceFile the File object.
     * @throws Exception on file access and parse errors.
     */
    public UgJsonDataSet(File sourceFile) throws Exception {
        super(sourceFile);
    }

    /**
     * {@inheritDoc} 
     */
    @Override
    protected void readSourceFile() throws Exception {
        List<Object> bodies = new LinkedList<>();
        JSONArray jsonObjects = (JSONArray) parser.parse(new FileReader(getSourceFile()));
        
        bodies.add(null);
        
        for (int i = 0; i < jsonObjects.size(); i++) {
            JSONObject jsonObject = (JSONObject) jsonObjects.get(i);
            bodies.add(jsonObject.toJSONString().getBytes());
        }

        bodies.add(null);
        setDefaultBodies(bodies);
        setJsonObjects(jsonObjects);
    }
    
    /**
     * {@inheritDoc} 
     */
    @Override
    protected void applyHeaders(Exchange exchange, long messageIndex) {
        Map<String, Object> headers = new HashMap<>();
        headers.put("JMSMessageID", String.valueOf(messageIndex));
        headers.put(UgMessage.Header.SequenceNumber, messageIndex);
        headers.put(UgMessage.Header.Version, 1);
        headers.put(UgMessage.Header.SyncType, UgMessage.SyncType.Incremental);

        if (isStartMessage(messageIndex)) {
            headers.put(UgMessage.Header.Operation, UgMessage.Operation.SyncStart);
        } else if (isDoneMessage(messageIndex)) {
            headers.put(UgMessage.Header.Operation, UgMessage.Operation.SyncDone);
        } else {
            JSONObject jsonObject = (JSONObject) getJsonObjects().get((int) (messageIndex % getDefaultBodies().size()) - 1);
            if (((Boolean) jsonObject.get("deleted")).booleanValue()) {
                headers.put(UgMessage.Header.Operation, UgMessage.Operation.Delete);
            } else {
                headers.put(UgMessage.Header.Operation, UgMessage.Operation.Update);
            }
            headers.put(UgMessage.Header.Class, jsonObject.get("ugClass"));
            headers.put(UgMessage.Header.Kthid, jsonObject.get("kthid"));
        }
        exchange.getIn().setHeaders(headers);
    }
    
    private boolean isStartMessage(long messageIndex) {
        return (messageIndex % (getDefaultBodies().size()) == 0);
    }

    private boolean isDoneMessage(long messageIndex) {
        return (messageIndex % (getDefaultBodies().size()) == (getDefaultBodies().size()-1));
    }
}
