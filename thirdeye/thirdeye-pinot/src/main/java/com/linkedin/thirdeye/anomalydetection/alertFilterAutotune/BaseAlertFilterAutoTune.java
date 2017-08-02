package com.linkedin.thirdeye.anomalydetection.alertFilterAutotune;

import com.linkedin.thirdeye.datalayer.dto.AutotuneConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detector.email.filter.AlertFilter;
import com.linkedin.thirdeye.detector.email.filter.PrecisionRecallEvaluator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class BaseAlertFilterAutoTune implements AlertFilterAutoTune {
  private final static Logger LOG = LoggerFactory.getLogger(BaseAlertFilterAutoTune.class);

  protected AutotuneConfigDTO autotuneConfig;
  protected List<MergedAnomalyResultDTO> trainingAnomalies;
  protected AlertFilter alertFilter;

  public void init(List<MergedAnomalyResultDTO> anomalies, AlertFilter alertFilter, AutotuneConfigDTO autotuneConfig) {
    this.trainingAnomalies = anomalies;
    this.autotuneConfig = autotuneConfig;
    this.alertFilter = alertFilter;
  }

  public PrecisionRecallEvaluator getEvaluator(AlertFilter alertFilter, List<MergedAnomalyResultDTO> anomalies){
    return new PrecisionRecallEvaluator(alertFilter, anomalies);
  }

  public AutotuneConfigDTO getAutotuneConfig() {
    return this.autotuneConfig;
  }

  public List<MergedAnomalyResultDTO> getTrainingAnomalies() {
    return this.trainingAnomalies;
  }

  public AlertFilter getCurrentAlertFilter() {
    return this.alertFilter;
  }
}
