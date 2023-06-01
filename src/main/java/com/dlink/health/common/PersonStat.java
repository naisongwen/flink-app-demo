package com.dlink.health.common;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class PersonStat implements Serializable {
    String personcardno;
    String vname;
}
