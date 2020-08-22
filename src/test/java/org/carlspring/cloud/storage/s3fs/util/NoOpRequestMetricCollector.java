package org.carlspring.cloud.storage.s3fs.util;

import com.amazonaws.Request;
import com.amazonaws.Response;
import com.amazonaws.metrics.RequestMetricCollector;

public class NoOpRequestMetricCollector
        extends RequestMetricCollector
{


    @Override
    public void collectMetrics(Request<?> request, Response<?> response)
    {
        //
    }

    @Override
    public boolean isEnabled()
    {
        return false;
    }

}
