package com.lehoon.elasticsearch.client.utils;

import java.text.DateFormat;
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
    private final static SimpleDateFormat sdfYear = new SimpleDateFormat("yyyy");

    private final static SimpleDateFormat sdfDay = new SimpleDateFormat(
            "yyyy-MM-dd");

    private final static SimpleDateFormat sdfDays = new SimpleDateFormat(
            "yyyyMMdd");

    private final static SimpleDateFormat sdfTime = new SimpleDateFormat(
            "yyyy-MM-dd HH:mm:ss");

    private final static SimpleDateFormat sdfMilli = new SimpleDateFormat(
            "yyyy-MM-dd HH:mm:ss.SSS");

    private final static SimpleDateFormat sdfMilli111 = new SimpleDateFormat(
            "yyyyMMddHHmmssSSS");

    /**
     * 获取YYYY格式
     *
     * @return
     */
    public static String getYear() {
        return sdfYear.format(new Date());
    }

    /**
     * 获取YYYY-MM-DD格式
     *
     * @return
     */
    public static String getDay() {
        return sdfDay.format(new Date());
    }

    /**
     * 获取YYYYMMDD格式
     *
     * @return
     */
    public static String getDays() {
        return sdfDays.format(new Date());
    }

    /**
     * 获取YYYY-MM-DD HH:mm:ss格式
     *
     * @return
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
     *
     * @return
     */
    public static String getFormatTime(Date date) {
        return sdfTime.format(date);
    }

    public static String getDateToString(Date date) {
        return sdfDays.format(date);
    }

    /**
     * @param s
     * @param e
     * @return boolean
     * @throws
     * @Title: compareDate
     * @Description: TODO(日期比较 ， 如果s > = e 返回true 否则返回false)
     * @author luguosui
     */
    public static boolean compareDate(String s, String e) {
        if (fomatDate(s) == null || fomatDate(e) == null) {
            return false;
        }
        return fomatDate(s).getTime() >= fomatDate(e).getTime();
    }

    /**
     * 格式化日期
     *
     * @return
     */
    public static Date fomatDate(String date) {
        DateFormat fmt = new SimpleDateFormat("yyyyMMdd");
        try {
            return fmt.parse(date);
        } catch (ParseException e) {
            //log.error(e.getMessage(), e);
            return null;
        }
    }

    /**
     * 根据参数格式化日期
     *
     * @return
     */
    public static Date fomatDate(String date, String type) {
        try {
            if ("sdfTime".equals(type)) {
                return sdfTime.parse(date);
            } else if ("sdfDay".equals(type)) {
                return sdfDay.parse(date);
            } else if ("sdfDays".equals(type)) {
                return sdfDays.parse(date);
            } else if ("sdfMilli".equals(type)) {
                return sdfMilli.parse(date);
            }
            return sdfYear.parse(date);
        } catch (ParseException e) {
            //log.error(e.getMessage(), e);
            return null;
        }
    }

    /**
     * 校验日期是否合法
     *
     * @return
     */
    public static boolean isValidDate(String s) {
        DateFormat fmt = new SimpleDateFormat("yyyy-MM-dd");
        try {
            fmt.parse(s);
            return true;
        } catch (Exception e) {
            // 如果throw java.text.ParseException或者NullPointerException，就说明格式不对
            return false;
        }
    }

    public static int getDiffYear(String startTime, String endTime) {
        DateFormat fmt = new SimpleDateFormat("yyyy-MM-dd");
        try {
            long aa = 0;
            int years = (int) (((fmt.parse(endTime).getTime() - fmt.parse(startTime).getTime()) / (1000 * 60 * 60 * 24)) / 365);
            return years;
        } catch (Exception e) {
            // 如果throw java.text.ParseException或者NullPointerException，就说明格式不对
            return 0;
        }
    }

    /**
     * <li>功能描述：时间相减得到天数
     *
     * @param beginDateStr
     * @param endDateStr
     * @return long
     * @author Administrator
     */
    public static long getDaySub(String beginDateStr, String endDateStr) {
        long day = 0;
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        Date beginDate = null;
        Date endDate = null;

        try {
            beginDate = format.parse(beginDateStr);
            endDate = format.parse(endDateStr);
        } catch (ParseException e) {
            //log.error(e.getMessage(), e);
        }
        day = (endDate.getTime() - beginDate.getTime()) / (24 * 60 * 60 * 1000);
        //System.out.println("相隔的天数="+day);

        return day;
    }

    /**
     * 得到n天之后的日期
     *
     * @param days
     * @return
     */
    public static String getAfterDayDate(String days) {
        int daysInt = Integer.parseInt(days);

        Calendar canlendar = Calendar.getInstance(); // java.util包
        canlendar.add(Calendar.DATE, daysInt); // 日期减 如果不够减会将月变动
        Date date = canlendar.getTime();

        SimpleDateFormat sdfd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateStr = sdfd.format(date);

        return dateStr;
    }

    /**
     * 得到n天之后是周几
     *
     * @param days
     * @return
     */
    public static String getAfterDayWeek(String days) {
        int daysInt = Integer.parseInt(days);

        Calendar canlendar = Calendar.getInstance(); // java.util包
        canlendar.add(Calendar.DATE, daysInt); // 日期减 如果不够减会将月变动
        Date date = canlendar.getTime();

        SimpleDateFormat sdf = new SimpleDateFormat("E");
        String dateStr = sdf.format(date);

        return dateStr;
    }

    public static String getHourDateTime(final int hour) {
        Calendar canlendar = Calendar.getInstance(); // java.util包
        canlendar.add(Calendar.HOUR, hour);
        SimpleDateFormat sdfd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdfd.format(canlendar.getTime());
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

    /***
     * @param date  : 时间字符串转换定时任务时间
     * @return
     */
    public static String getCron(Date  date){
        //String dateFormat="s mm HH * * *";
        String dateFormat="s mm HH ? * * ";
        return formatDateByPattern(date, dateFormat);
    }

    /***
     *  修改cron参数格式
     * @param date
     * @param dateFormat : e.g:yyyy-MM-dd HH:mm:ss
     * @return
     */
    public static String formatDateByPattern(Date date, String dateFormat){
        SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
        String formatTimeStr = "";
        if (date != null) {
            formatTimeStr = sdf.format(date);
        }
        return formatTimeStr;
    }
}
