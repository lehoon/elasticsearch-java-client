package com.finesys.elasticsearch.client.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * <p>Title: </p>
 * <p>Description: </p>
 * <p>Copyright: CopyRight (c) 2020-2035</p>
 * <p>Company: lehoon Co. LTD.</p>
 * <p>Author: lehoon</p>
 * <p>Date: 2022/6/20 15:53</p>
 */
public class DateUtils {
    private final static SimpleDateFormat sdfDepthDay = new SimpleDateFormat(
            "yyyyMMdd");

    private final static SimpleDateFormat sdfDay = new SimpleDateFormat(
            "yyyy-MM-dd");

    private final static SimpleDateFormat sdfTime = new SimpleDateFormat(
            "yyyy-MM-dd HH:mm:ss");

    private final static SimpleDateFormat sdfMilli = new SimpleDateFormat(
            "yyyy-MM-dd HH:mm:ss.SSS");

    private final static SimpleDateFormat sdfMilli111 = new SimpleDateFormat(
            "yyyyMMddHHmmssSSS");

    /**
     * 获取YYYY-MM-DD格式
     */
    public static String getDay() {
        return sdfDay.format(new Date());
    }

    /**
     * 获取YYYY-MM-DD HH:mm:ss格式
     */
    public static String getTime() {
        return sdfTime.format(new Date());
    }

    public static Date getNow() {
        return Calendar.getInstance().getTime();
    }

    public static String getDepthTime(final Date now) {
        return sdfMilli.format(now);
    }

    public static String getDepthIdTime() {
        return sdfMilli111.format(Calendar.getInstance().getTime());
    }

    public static long fromStringFormat(final String datetime) {
        try {
            Date date =  sdfMilli.parse(datetime);
            return date.getTime();
        } catch (ParseException e) {
            return 0;
        }
    }

    /**
     * YYYY-MM-DD HH:mm:ss格式
     */
    public static String getFormatTime(Date date) {
        return sdfTime.format(date);
    }

    /**
     * 格式化日期
     */
    public static Date fomatDate(String date) {
        try {
            return sdfDay.parse(date);
        } catch (ParseException e) {
            return null;
        }
    }

    public static Date formatDepthPlaybackDate(String date) {
        try {
            return sdfDepthDay.parse(date);
        } catch (ParseException e) {
            return null;
        }
    }

    public static String formatDepthPlaybackDate(Date date) {
        return sdfMilli.format(date);
    }

    /**
     * 14位现在日期时间
     */
    public static String now14() {
        Calendar canlendar = Calendar.getInstance(); // java.util包
        return new SimpleDateFormat("yyyyMMddHHmmss").format(canlendar.getTime());
    }

    public static Date now() {
        Calendar canlendar = Calendar.getInstance(); // java.util包
        return canlendar.getTime();
    }

    public static long toDayLastTime(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.add(Calendar.DAY_OF_MONTH, 1);
        return calendar.getTime().getTime();
    }

    public static long toDayBeginTime(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.add(Calendar.DAY_OF_MONTH, -1);
        return calendar.getTime().getTime();
    }

    public static long nextDayZeroTime(final String today) {
        Date todayDate = fomatDate(today);
        return toDayLastTime(todayDate);
    }

    public static long upDayZeroTime(final String today) {
        Date todayDate = fomatDate(today);
        return toDayBeginTime(todayDate);
    }

    public static String nextDay() {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.HOUR, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        return sdfDay.format(calendar.getTime());
    }

    public static String nextDay(final String currentDay) {
        Date todayDate = fomatDate(currentDay);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(todayDate);
        calendar.add(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.HOUR, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        return sdfDay.format(calendar.getTime());
    }

    public static Date nextDay(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.HOUR, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        return calendar.getTime();
    }

    public static Date formatDepthDate(final String depthTime) {
        try {
            return sdfMilli.parse(depthTime);
        } catch (ParseException e) {
            return  null;
        }
    }

    public static long depthTime(final String time) {
        Date todayDate = formatDepthDate(time);
        if (todayDate == null) return 0l;
        return todayDate.getTime();
    }

    public static String depthPlaybackBeginTime(final String time) {
        Date date = formatDepthPlaybackDate(time);
        if (date == null) return null;
        return formatDepthPlaybackDate(date);
    }

    public static String depthPlaybackEndTime(final String time) {
        Date date = formatDepthPlaybackDate(time);
        if (date == null) return null;
        return formatDepthPlaybackDate(date);
    }

    public static String playbackDateTime(final String time) {
        Date date = formatDepthDate(time);
        if (date == null) {
            date = now();
        }

        return sdfDay.format(date.getTime());
    }

    public static String playbackCurrentDateTime(final String dateTime) {
        Date date = formatDepthPlaybackDate(dateTime);
        if (date == null) {
            date = now();
        }

        return sdfDay.format(date.getTime());
    }
}
