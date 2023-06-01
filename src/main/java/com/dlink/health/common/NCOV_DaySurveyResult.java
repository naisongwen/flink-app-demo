package com.dlink.health.common;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Date;

@Getter
@Setter
public class NCOV_DaySurveyResult extends SysEmployee {
    String ResultOID ;
    String Badge ;
    int Eid;
    long TimeSpan;
    short IsValid;
    String  CreateBy ;
    Date CreateDate;
    String UpdateBy ;
    Date UpdateDate ;
    String ShowName ;
    String SubmissionDate ;
    String IsWork ;
    String EstimateDate ;
    String MorningTemperature ;
    String AfternoonTemperature ;
    String MySymptom ;
    String FriendSymptom ;
    String EmpHealthStatus ;
    String Q1 ;
    String Q2 ;
    String Q3 ;
    String Q4 ;
    String Q5 ;
    String Q6 ;
    String Q7 ;
    String Q8 ;
    String Q9 ;
    String Q10 ;
    String Q11 ;
    String Q12 ;
    String Q13 ;
    String Q14 ;
    String Q15 ;
    String Q16 ;
    String Q17 ;
    String Q18 ;
    String Q19 ;
    String Q20 ;
    String Q21 ;
    String Q30 ;
    char TotalEmpHealthStatus ;
}
