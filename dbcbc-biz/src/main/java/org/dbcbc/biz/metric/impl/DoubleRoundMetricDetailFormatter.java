package org.dbcbc.biz.metric.impl;

import org.dbcbc.biz.metric.AbstractMetricDetailFormatter;
import org.dbcbc.biz.model.Sample;
import org.dbcbc.biz.vo.MetricResponseVO;

public final class DoubleRoundMetricDetailFormatter extends AbstractMetricDetailFormatter {

    @Override
    public void apply(MetricResponseVO vo) {
        Sample sample = vo.getMeasurements().get(0);
        long round = Math.round((Double) sample.getValue());
        vo.setDetail(String.valueOf(round));
    }

}