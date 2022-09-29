package com.finesys.playback.es.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: CopyRight (c) 2020-2035</p>
 * <p>Company: finesys Co. LTD.</p>
 * <p>Author: lehoon</p>
 * <p>Date: 2022/6/23 9:50</p>
 */
@Setter
@Getter
@NoArgsConstructor
public class EsDataModel<T> {
    private String id;
    private T data;
}
