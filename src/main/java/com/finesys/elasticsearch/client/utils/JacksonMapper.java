package com.finesys.elasticsearch.client.utils;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.text.SimpleDateFormat;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: CopyRight (c) 2020-2035</p>
 * <p>Company: lehoon Co. LTD.</p>
 * <p>Author: lehoon</p>
 * <p>Date: 2022/6/20 16:44</p>
 */
public class JacksonMapper extends ObjectMapper {

    private static final long serialVersionUID = 27476334584914123L;

    public JacksonMapper() {
    }

    /**
     * 带是否时间格式化的构造方法
     *
     * @param dateFormat 格式如:yyyy-MM-dd
     */
    public JacksonMapper(String dateFormat) {
        setDateFormat(new SimpleDateFormat(dateFormat));
    }

    /**
     * 把Object转化为JSON字符串
     *
     * @param object can be pojo entity,list,map etc.
     */
    public String toJson(Object object) {
        try {
            return this.writeValueAsString(object);
        } catch (Exception e) {
            //log.error(e.getMessage(), e);
            throw new RuntimeException("对象转化为JSON时,解析对象错误");
        }
    }

    /**
     * json转换为java对象
     *
     * @param json  字符串
     * @param clazz 对象的class
     * @return 返回对象
     */
    public <T> T toObject(String json, Class<T> clazz) {
        try {
            return this.readValue(json, clazz);
        } catch (Exception e) {
            //log.error(e.getMessage(),e);
            throw new RuntimeException("JSON转化为对象时,解析JSON错误");
        }
    }
}