/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package se.kth.infosys.camel;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.camel.Exchange;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import se.kth.infosys.camel.ug.UgMessage;

/**
 * A DataSet that reads JSON encoded UG payloads from a file.
 */
public class UgJsonDataSet extends JsonDataSet {
    protected static final JSONParser parser = new JSONParser();
    
    public UgJsonDataSet() {}

    public UgJsonDataSet(String sourceFileName) throws IOException, ParseException {
        super(sourceFileName);
    }

    public UgJsonDataSet(File sourceFile) throws IOException, ParseException {
        super(sourceFile);
    }

    @Override
    protected void readSourceFile() throws IOException, ParseException {
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
