package com.dlink.health.common;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class SysEmployee implements Serializable {
    int Eid;
    String Badge ;
    String name ;
}
