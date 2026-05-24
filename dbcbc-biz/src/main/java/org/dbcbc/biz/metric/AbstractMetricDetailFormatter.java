package org.dbcbc.biz.metric;

import org.dbcbc.biz.vo.MetricResponseVO;
import org.dbcbc.common.util.CollectionUtils;

public abstract class AbstractMetricDetailFormatter implements MetricDetailFormatter {

    protected abstract void apply(MetricResponseVO vo);

    @Override
    public void format(MetricResponseVO vo) {
        if (CollectionUtils.isEmpty(vo.getMeasurements())) {
            return;
        }
        apply(vo);
    }
}
