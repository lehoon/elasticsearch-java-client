package com.finesys.playback.es.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
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

    public static String formatSdfDay(Date date) {
        return sdfDay.format(date);
    }

    /**
     * 获取YYYY-MM-DD HH:mm:ss格式
     */
    public static String getTime() {
        return sdfTime.format(new Date());
    }

    public static Date getSdfTime(String dateString) {
        try {
            return sdfTime.parse(dateString);
        } catch (ParseException e) {
            return null;
        }
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
            Date date = sdfMilli.parse(datetime);
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

    public static Date formatDepthPlaybackDate1(final String datetime) {
        try {
            return sdfMilli.parse(datetime);
        } catch (ParseException e) {
            return null;
        }
    }

    public static long subDateTime(Date beginTime, Date endTime) {
        Calendar calendar1 = Calendar.getInstance();
        Calendar calendar2 = Calendar.getInstance();
        calendar1.setTime(beginTime);
        calendar2.setTime(endTime);

        LocalDateTime localDateTime1 = LocalDateTime.of(calendar1.get(Calendar.YEAR),
                calendar1.get(Calendar.MONTH) + 1,
                calendar1.get(Calendar.DAY_OF_MONTH) + 1,
                calendar1.get(Calendar.HOUR_OF_DAY),
                calendar1.get(Calendar.MINUTE));

        LocalDateTime localDateTime2 = LocalDateTime.of(calendar2.get(Calendar.YEAR),
                calendar2.get(Calendar.MONTH) + 1,
                calendar2.get(Calendar.DAY_OF_MONTH) + 1,
                calendar2.get(Calendar.HOUR_OF_DAY),
                calendar2.get(Calendar.MINUTE));
        Duration duration = Duration.between(localDateTime1, localDateTime2);
        return duration.toDays();
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

    public static String beforeDay(final Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DAY_OF_MONTH, -1);
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

    public static Date getMarketBegineTime(Date date, int sub) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DAY_OF_MONTH, sub);
        calendar.set(Calendar.HOUR, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        return calendar.getTime();
    }

    public static Date getMarketEndTime(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR, 23);
        calendar.set(Calendar.MINUTE, 59);
        calendar.set(Calendar.SECOND, 59);
        return calendar.getTime();
    }

    public static Date formatDepthDate(final String depthTime) {
        try {
            return sdfMilli.parse(depthTime);
        } catch (ParseException e) {
            return null;
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

    public static boolean isSameDay(final Date dateOne, final Date dateTwo) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(dateOne);
        Calendar calendar1 = Calendar.getInstance();
        calendar1.setTime(dateTwo);
        return calendar.get(Calendar.YEAR) == calendar1.get(Calendar.YEAR) &&
                calendar.get(Calendar.MONTH) == calendar1.get(Calendar.MONTH) &&
                calendar.get(Calendar.DAY_OF_MONTH) == calendar1.get(Calendar.DAY_OF_MONTH);
    }

    public static boolean isWorkDay(final Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);

        return calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY ||
                calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY;
    }

    /***转换日期格式为统一格式*/
    public static String commonDate(String dateStr) {
        //返回新的字符串
        String newDate = dateStr;
        //去除中横线
        newDate=newDate.replaceAll("-", "");
        //去除斜杠
        newDate=newDate.replaceAll("/", "");
        return newDate;
    }
}
