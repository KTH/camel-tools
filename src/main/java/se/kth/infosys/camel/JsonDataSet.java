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
import java.util.LinkedList;
import java.util.List;

import org.apache.camel.component.dataset.ListDataSet;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * A ListDataSet that reads JSON encoded payloads from a file.
 */
public class JsonDataSet extends ListDataSet {
    protected static final JSONParser parser = new JSONParser();

    private JSONArray jsonObjects = new JSONArray();
    private File sourceFile;
    
    public JsonDataSet() {}

    public JsonDataSet(String sourceFileName) throws IOException, ParseException {
        this(new File(sourceFileName));
    }

    public JsonDataSet(File sourceFile) throws IOException, ParseException {
        setSourceFile(sourceFile);
    }

    public File getSourceFile() {
        return sourceFile;
    }

    public void setSourceFile(File sourceFile) throws IOException, ParseException {
        this.sourceFile = sourceFile;
        readSourceFile();
    }

    public JSONArray getJsonObjects() {
        return jsonObjects;
    }

    public void setJsonObjects(JSONArray jsonObjects) {
        this.jsonObjects = jsonObjects;
    }

    protected void readSourceFile() throws IOException, ParseException {
        List<Object> bodies = new LinkedList<>();
        jsonObjects = (JSONArray) parser.parse(new FileReader(sourceFile));
        
        for (int i = 0; i < jsonObjects.size(); i++) {
            JSONObject jsonObject = (JSONObject) jsonObjects.get(i);
            bodies.add(jsonObject.toJSONString().getBytes());
        }
        setDefaultBodies(bodies);
    }
}
