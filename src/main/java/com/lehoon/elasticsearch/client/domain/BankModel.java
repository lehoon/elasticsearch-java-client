package com.lehoon.elasticsearch.client.domain;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: CopyRight (c) 2020-2035</p>
 * <p>Company: lehoon Co. LTD.</p>
 * <p>Author: lehoon</p>
 * <p>Date: 2022/6/17 16:40</p>
 */
@Setter
@Getter
@ToString
@NoArgsConstructor
public class BankModel implements Serializable {
    private Integer accountNumber;
    private Integer lalance;
    private String firstName;
    private String lastName;
    private Integer age;
    private String gender;
    private String address;
    private String employer;
    private String email;
    private String city;
    private String state;
}
