package sqlmapping;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Map;
import java.util.logging.Logger;
import maptree.Mapifier;

/**
 * Provides utilities for transforming SQL data.
 *
 * <p>
 *   <b>Details:</b>
 *   Provides utilities for transforming SQL data before it is updated or after it is retreived.
 * </p>
 */
abstract public class Transformer {
  private static final Logger logger = Logger.getLogger("mcore-debug");

  abstract public Object transform(Object obj, Map siblings);

  private static final String DEFAULT_DATE_FORMAT       = "dd MMM yyyy";
  private static final String YEAR_DATE_FORMAT          = "yyyy";
  private static final Integer JAVA_MONTH_OFFSET        = 1;

  public static String toYn(Object obj){
    return Transformer.toBool(obj) ? "Y" : "N";
  }
  public static boolean toBool(Object obj){
    if(obj == null || obj.equals(Mapifier.UNDEFINED)){
      return false;
    }
    String s = obj.toString().toLowerCase();
    return !(
      s.equals("false") ||
        s.equals("f") ||
        s.equals("no") ||
        s.equals("n") ||
        s.equals("0") ||
        s.equals("")
    );
  }
  public static boolean strToBool(Object obj){
    if(obj == null || obj.toString().isEmpty() || obj.equals(Mapifier.UNDEFINED) ){
      return false;
    }
    return true;
  }

  public static Integer toInt(Object obj){
    if(obj == null){
      return null;
    }
    try{
      return Integer.parseInt(obj.toString());
    }catch(NumberFormatException e){
      return null;
    }
  }

  // Todo - make this more effectively ensure 8 digits
  public static Object toYearOrNull(Object obj) {
    if (obj == null) {
      return null;
    }

    try {
      int year = Integer.parseInt(obj.toString());

      if (year <= 9999) {
        return year * 10000;
      }

      return year;

    } catch(NumberFormatException e) {
      return null;
    }
  }

  public static int toIntOrDefault(Object obj, int iDefault){
    if(obj == null){
      return iDefault;
    }
    try {
      return Integer.parseInt(obj.toString());
    }catch(NumberFormatException e){
      return iDefault;
    }
  }
  public static String formatDate(Object obj){
    return formatDate(obj, DEFAULT_DATE_FORMAT);
  }
  public static String formatDate(Object obj, String format){
    if(obj == null){
      return "";
    }
    SimpleDateFormat formatter = new SimpleDateFormat(format);
    if(obj instanceof Date){
      return formatter.format(obj);
    }else if(obj.toString().length() == 8){
      String vDate = obj.toString();
      Integer year = Integer.parseInt(vDate.substring(0, 4));
      Integer month = Integer.parseInt(vDate.substring(4, 6)) - JAVA_MONTH_OFFSET;
      Integer day = Integer.parseInt(vDate.substring(6, 8));
      Calendar cal = Calendar.getInstance();
      cal.set(year, month, day);
      return formatter.format(cal.getTime());
    }
    return "";
  }
  public static Date toDate(Object obj){
    if(obj == null){
      return null;
    }
    if(obj instanceof Integer && obj.toString().length() == 8){
      String vDate = obj.toString();
      Integer year = Integer.parseInt(vDate.substring(0, 4));
      Integer month = Integer.parseInt(vDate.substring(4, 6)) - JAVA_MONTH_OFFSET;;
      Integer day = Integer.parseInt(vDate.substring(6, 8));
      Calendar cal = Calendar.getInstance();
      cal.set(year, month, day);
      return cal.getTime();
    }
    String format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    if(obj.toString().length() == DEFAULT_DATE_FORMAT.length()){
      format = DEFAULT_DATE_FORMAT;
    }
    SimpleDateFormat parser = new SimpleDateFormat(format);
    try {
      return parser.parse(obj.toString());
    }catch(ParseException e){
      return null;
    }
  }
  public static Integer toIntDate(Object obj){
    if(obj == null){
      return null;
    }
    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
    if(obj instanceof Date){
      try {
        return Integer.parseInt(formatter.format(obj));
      }catch(NumberFormatException e){
        return null;
      }
    }else {
      String format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
      if(obj.toString().length() == DEFAULT_DATE_FORMAT.length()){
        format = DEFAULT_DATE_FORMAT;
      }
      SimpleDateFormat parser = new SimpleDateFormat(format);
      try {
        Date vDate = parser.parse(obj.toString());
        return Integer.parseInt(formatter.format(vDate));
      }catch(ParseException e){
        return null;
      }
    }
  }
  public static Integer yearToIntDate(Object obj){
    if(obj == null || obj.toString().length() != 4){
      return null;
    }
    try{
      int year = Integer.parseInt(obj.toString());
      return (year * 10000) + 101; // adding 101 gives us the first of january
    }catch(NumberFormatException e){
      return null;
    }
  }

  public static final Transformer DATE_TO_YEAR          = new DateToYear();
  public static final Transformer YEAR_TO_DATE          = new YearToDate();
  /**
   * Formats either a date or an integer that can be parsed as a date (e.g. 20070531), putting it in the format "dd MMM yyyy".
   */
  public static final Transformer FORMAT_DATE           = new FormatDate();
  public static final Transformer TO_DATE               = new ToDate();
  public static final Transformer TO_INTDATE            = new ToIntDate();
  public static final Transformer YEAR_TO_INTDATE       = new YearToIntDate();
  public static final Transformer TO_STR_OR_EMPTY       = new ToStrOrEmpty();
  public static final Transformer TO_YN                 = new ToYn();
  public static final Transformer TO_BOOL               = new ToBoolean();
  public static final Transformer STR_TO_BOOL           = new StrToBoolean();

  private static class DateToYear extends Transformer {
    public Object transform(Object obj, Map siblings){
      return formatDate(obj, YEAR_DATE_FORMAT);
    }
  }
  private static class YearToDate extends Transformer {
    public Object transform(Object obj, Map siblings){
      if(obj == null || obj.toString().isEmpty()){
        return null;
      }
      try {
        Calendar calendar = GregorianCalendar.getInstance();
        int year = Integer.parseInt(obj.toString());
        calendar.set(year, Calendar.JANUARY, 1);
        return calendar.getTime();
      } catch(NumberFormatException e){
        return null;
      }
    }
  }
  private static class FormatDate extends Transformer {
    @Override
    public Object transform(Object obj, Map siblings) {
      return formatDate(obj);
    }
  }
  private static class ToDate extends Transformer {
    @Override
    public Object transform(Object obj, Map siblings) {
      return toDate(obj);
    }
  }
  private static class ToIntDate extends Transformer {
    @Override
    public Object transform(Object obj, Map siblings) {
      return toIntDate(obj);
    }
  }
  private static class YearToIntDate extends Transformer {
    @Override
    public Object transform(Object obj, Map siblings) {
      return yearToIntDate(obj);
    }
  }
  private static class ToStrOrEmpty extends Transformer {
    public Object transform(Object obj, Map siblings){
      if(obj == null){
        return "";
      }
      return obj.toString();
    }
  }
  private static class ToYn extends Transformer {
    public Object transform(Object obj, Map siblings){
      return toYn(obj);
    }
  }
  private static class ToBoolean extends Transformer {
    @Override
    public Object transform(Object obj, Map siblings) {
      return toBool(obj);
    }
  }
  private static class StrToBoolean extends Transformer {
    @Override
    public Object transform(Object obj, Map siblings){
      return strToBool(obj);
    }
  }
}
