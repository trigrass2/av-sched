package net.airvantage.sched.app.exceptions;

import java.util.Arrays;

public class AppExceptions {

    public static AppException jobNotFound(String jobId) {
        return new AppException("job.not.found", Arrays.asList(jobId));
    }

    public static AppException serverError(Throwable cause) {
        return new AppException("unexpected.error", cause);
    }

}
