package net.airvantage.sched.app.exceptions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AppException extends Exception {
    
    private static final long serialVersionUID = 1L;

    public String error;
    public List<String> params;

    public AppException(String error, Throwable e) {
        super(e);
        this.error = error;
        this.params = new ArrayList<String>();
    }
    
    public AppException(String error, List<String> params) {
        super();
        this.error = error;
        this.params = params;
    }
    
    public AppException(String error, String... params) {
        super();
        this.error = error;
        this.params = Arrays.asList(params);
    }
    
    public AppException(String error) {
        super();
        this.error = error;
    }

    public AppException(String error, List<String> params, Throwable cause) {
        super(cause);
        this.error = error;
        this.params = params;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public List<String> getParams() {
        return params;
    }

    public void setParams(List<String> params) {
        this.params = params;
    }
    
    public Map<String, Object> asMap() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("error", error);
        map.put("params", params);
        return map;
    }

}
