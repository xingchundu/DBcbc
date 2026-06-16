/**
 * DBSyncer Copyright 2020-2024 All Rights Reserved.
 */
package org.dbcbc.connector.dm.schema.support;

import org.dbcbc.common.util.DateFormatUtil;
import org.dbcbc.common.util.StringUtil;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 达梦 LogMiner redo 中日期/时间字面量解析（兼容 TO_DATE、DATE '...'、纯日期字符串）。
 */
public final class DmDateParseUtil {

    private static final Pattern DATE_ONLY = Pattern.compile("^\\d{4}-\\d{1,2}-\\d{1,2}$");
    private static final Pattern TO_DATE = Pattern.compile("(?i)TO_DATE\\s*\\(\\s*'([^']+)'");
    private static final Pattern DATE_LITERAL = Pattern.compile("(?i)^DATE\\s*'([^']+)'$");
    private static final Pattern TIMESTAMP_LITERAL = Pattern.compile("(?i)^TIMESTAMP\\s*'([^']+)'$");

    private DmDateParseUtil() {
    }

    public static Date parseDate(String raw) {
        Timestamp timestamp = parseTimestamp(raw);
        if (timestamp == null) {
            return null;
        }
        return new Date(timestamp.getTime());
    }

    public static Timestamp parseTimestamp(String raw) {
        if (StringUtil.isBlank(raw)) {
            return null;
        }
        String s = unwrapDateExpression(raw.trim());
        if (StringUtil.isBlank(s)) {
            return null;
        }
        if (DATE_ONLY.matcher(s).matches()) {
            return Timestamp.valueOf(LocalDate.parse(normalizeDateOnly(s)).atStartOfDay());
        }
        try {
            return DateFormatUtil.stringToTimestamp(s);
        } catch (RuntimeException e) {
            return tryFlexibleTimestamp(s);
        }
    }

    private static String unwrapDateExpression(String s) {
        Matcher toDate = TO_DATE.matcher(s);
        if (toDate.find()) {
            return toDate.group(1);
        }
        Matcher dateLiteral = DATE_LITERAL.matcher(s);
        if (dateLiteral.matches()) {
            return dateLiteral.group(1);
        }
        Matcher timestampLiteral = TIMESTAMP_LITERAL.matcher(s);
        if (timestampLiteral.matches()) {
            return timestampLiteral.group(1);
        }
        if (s.length() >= 2 && s.charAt(0) == '\'' && s.charAt(s.length() - 1) == '\'') {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

    private static String normalizeDateOnly(String s) {
        String[] parts = s.split("-");
        if (parts.length != 3) {
            return s;
        }
        return String.format("%04d-%02d-%02d", Integer.parseInt(parts[0]), Integer.parseInt(parts[1]), Integer.parseInt(parts[2]));
    }

    private static Timestamp tryFlexibleTimestamp(String s) {
        try {
            return Timestamp.valueOf(LocalDateTime.parse(s, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        } catch (DateTimeParseException ignored) {
        }
        try {
            return Timestamp.valueOf(LocalDate.parse(s, DateTimeFormatter.ISO_LOCAL_DATE).atStartOfDay());
        } catch (DateTimeParseException ignored) {
        }
        return null;
    }
}
