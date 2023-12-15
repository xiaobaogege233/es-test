package cn.zchd.entity;

import lombok.Data;

import java.util.List;

@Data
public class TestBean {

    private String realname;

    private String idcard;

    private List<String> zonecodeList;

    private List<String> businesstypeList;

    private List<String> savemonthList;
}
