package com.agentlog.demo;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;

public class ReportConf {
    public String Tenant;
    public String Environment;
    public String StartEvent;
    public String EndEvent;
    public String MetricType;
    public String ReportField;
    public String Interval;
    public String ObjectType;
    public String Filter; //This may change to a JSONObject
}
