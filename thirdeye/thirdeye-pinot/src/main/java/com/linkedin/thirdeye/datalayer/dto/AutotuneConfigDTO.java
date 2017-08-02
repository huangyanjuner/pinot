package com.linkedin.thirdeye.datalayer.dto;

import com.linkedin.thirdeye.anomaly.detection.lib.AutotuneMethodType;
import com.linkedin.thirdeye.anomalydetection.performanceEvaluation.PerformanceEvaluationMethod;
import com.linkedin.thirdeye.datalayer.pojo.AutotuneConfigBean;
import com.linkedin.thirdeye.detector.email.filter.AlertFilter;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AutotuneConfigDTO extends AutotuneConfigBean {
  private static final Logger LOGGER = LoggerFactory.getLogger(AutotuneConfigDTO.class);
  public AutotuneConfigDTO() {

  }

  public AutotuneConfigDTO(AutotuneMethodType autotuneMethod, String userDefinedPattern){
    setAutotuneMethod(autotuneMethod);
    setUserDefinedPattern(userDefinedPattern);
  }

}
