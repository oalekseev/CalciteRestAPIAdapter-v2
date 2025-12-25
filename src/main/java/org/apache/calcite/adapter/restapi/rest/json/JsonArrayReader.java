package org.apache.calcite.adapter.restapi.rest.json;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.apache.calcite.adapter.restapi.rest.interfaces.ArrayReader;

public class JsonArrayReader implements ArrayReader {
    private final String json;

    public JsonArrayReader(String json) {
        this.json = json;
    }

    @Override
    public JSONArray read(String path) {
        if (json == null || json.isEmpty()) {
            return null;
        }
        try {
            Object responseObj = JsonPath.read(json, path);
            if (responseObj instanceof JSONArray) {
                return (JSONArray) responseObj;
            } else if (responseObj instanceof JSONObject) {
                // If the response is a single JSON object, wrap it in an array for processing
                JSONArray singleItemArray = new JSONArray();
                singleItemArray.add(responseObj);
                return singleItemArray;
            } else if (responseObj != null) {
                // If it's any other type of object, try to wrap it in an array
                JSONArray singleItemArray = new JSONArray();
                singleItemArray.add(responseObj);
                return singleItemArray;
            } else {
                return null;
            }
        } catch (PathNotFoundException e) {
            return null;
        }
    }
}
