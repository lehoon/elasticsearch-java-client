package com.finesys.playback.es.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Date;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: CopyRight (c) 2020-2035</p>
 * <p>Company: lehoon Co. LTD.</p>
 * <p>Author: lehoon</p>
 * <p>Date: 2022/6/20 15:36</p>
 */
@Setter
@Getter
@ToString
@NoArgsConstructor
public class DepthModel implements Serializable {
    private String symbol;
    private String type;
    private String market;
    private String data;
    private String time;
    private Date created;
}
