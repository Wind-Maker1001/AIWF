package com.aiwf.base.service;

import com.fasterxml.jackson.databind.ObjectMapper;

public final class JsonUtil {
    private static final ObjectMapper M = new ObjectMapper();
    private JsonUtil(){}

    public static String toJson(Object o) {
        try { return M.writeValueAsString(o); }
        catch (Exception e) { return "{}"; }
    }
}
