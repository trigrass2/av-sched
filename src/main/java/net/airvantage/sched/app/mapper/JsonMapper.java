package net.airvantage.sched.app.mapper;

import java.io.IOException;
import java.io.InputStream;

import net.airvantage.sched.app.exceptions.AppException;
import net.airvantage.sched.model.JobDef;
import net.airvantage.sched.model.JobId;
import net.airvantage.sched.model.PostHttpJobResult;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * A JSON mapper to convert data model object. No validation or default values are managed here.
 */
public class JsonMapper {

    private ObjectMapper jsonMapper;

    public JsonMapper() {

        jsonMapper = new ObjectMapper();
        jsonMapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    public JobDef jobDef(InputStream is) throws AppException {

        try {
            return jsonMapper.reader(JobDef.class).readValue(is);

        } catch (IOException ioex) {
            throw new AppException("invalid.json", ioex);
        }
    }

    public JobId jobId(InputStream is) throws AppException {

        try {
            return jsonMapper.reader(JobId.class).readValue(is);

        } catch (IOException ioex) {
            throw new AppException("invalid.json", ioex);
        }
    }

    public String writeValueAsString(Object object) throws IOException {
        return jsonMapper.writeValueAsString(object);
    }

    public PostHttpJobResult postHttpJobResult(InputStream is) throws AppException {

        try {
            return jsonMapper.reader(PostHttpJobResult.class).readValue(is);

        } catch (IOException ioex) {
            throw new AppException("invalid.json", ioex);
        }
    }

}
