package com.dlink.health.common;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Date;

@Getter
@Setter
public class AkmResult extends SysEmployee {
    String AkmResultOID;
    int Eid;
    Date datetime;
    char HealthState;
    String SyncMsg;
    short IsValid;
    Date CreateDate;
}
