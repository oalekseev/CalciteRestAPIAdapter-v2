package org.apache.calcite.adapter.restapi.rest.json;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import org.apache.calcite.adapter.restapi.rest.interfaces.ArrayParamReader;

public class JsonArrayParamReader implements ArrayParamReader {
    private final Object object;

    public JsonArrayParamReader(Object object) {
        this.object = object;
    }

    @Override
    public Object read(int index, String path) {
        if (object == null) {
            return null;
        }
        try {
            // If path doesn't start with '$', add it for JsonPath
            String jsonPath = path.startsWith("$") ? path : "$." + path;
            return JsonPath.read(object, jsonPath);
        } catch (PathNotFoundException e) {
            return null;
        }
    }
}
