package cn.zchd.entity;

import lombok.Data;

import java.math.BigDecimal;

/**
 * Copyright: 中传互动 & 卓讯互动
 * Author: liyaguang
 * Date: 2022/08/24
 */
@Data
public class HappinessrecordEntity{


    private Long id;

    private String businesstype = "";

    private String businesstypestr = "";

    private String savemonth = "";

    private String zonecode = "";

    private String address = "";

    private String realname = "";

    private String idcard = "";

    private String headname = "";

    private String headcard = "";

    private int membercount;

    private BigDecimal amount;

    private long userid;

    private long createtime;

    private long starttime;

    private long endtime;

}
