package org.dbcbc.biz.metric.impl;

import org.dbcbc.biz.metric.AbstractMetricDetailFormatter;
import org.dbcbc.biz.model.Sample;
import org.dbcbc.biz.vo.MetricResponseVO;

import java.util.List;

public final class GCMetricDetailFormatter extends AbstractMetricDetailFormatter {

    @Override
    public void apply(MetricResponseVO vo) {
        List<Sample> list = vo.getMeasurements();
        long count = Math.round((Double) list.get(0).getValue());
        String total = String.format("%.2f", ((Double) list.get(1).getValue()));
        long max = Math.round((Double) list.get(2).getValue());
        vo.setDetail(String.format("%s次，耗时:%s秒，最长:%s秒", count, total, max));
    }

}