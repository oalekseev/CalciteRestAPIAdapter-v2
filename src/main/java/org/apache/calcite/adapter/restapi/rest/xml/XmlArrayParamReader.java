package org.apache.calcite.adapter.restapi.rest.xml;

import org.apache.calcite.adapter.restapi.rest.interfaces.ArrayParamReader;

import java.util.Map;

public class XmlArrayParamReader implements ArrayParamReader {
    private final Object object;

    public XmlArrayParamReader(Object object) {
        this.object = object;
    }

    @Override
    public Object read(int index, String path) {
        Object curr = object;
        for (String part : path.split("\\.")) {
            if (curr instanceof Map) {
                curr = ((Map<?, ?>) curr).get(part);
            }
        }
        return curr;
    }
}
