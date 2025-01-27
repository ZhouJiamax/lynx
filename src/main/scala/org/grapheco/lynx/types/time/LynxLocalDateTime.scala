package org.grapheco.lynx.types.time

import org.grapheco.lynx.types.{LTLocalDateTime, LocalDateTimeType, LynxValue}
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural.LynxPropertyKey
import org.grapheco.lynx.types.time.LynxComponentDate.{getYearMonthDay, transformDate, transformYearOrdinalDay, transformYearQuarterDay, transformYearWeekDay, truncateDate}
import org.grapheco.lynx.types.time.LynxComponentTime.{getHourMinuteSecond, getNanosecond, truncateTime}
import org.grapheco.lynx.types.time.LynxComponentTimeZone.{getZone, truncateZone}
import org.grapheco.lynx.types.time.LynxDateTime.of
import org.grapheco.lynx.types.traits.HasProperty
import org.grapheco.lynx.util.LynxTemporalParser.splitDateTime
import org.grapheco.lynx.util.{LynxTemporalParseException, LynxTemporalParser}

import java.time.temporal.{ChronoUnit, TemporalUnit}
import java.time.{LocalDateTime, LocalTime, ZoneId}
import java.util.{Calendar, GregorianCalendar}

/**
 * @ClassName LynxLocalDateTime
 * @Description TODO
 * @Author huchuan
 * @Date 2022/4/1
 * @Version 0.1
 */
case class LynxLocalDateTime(localDateTime: LocalDateTime) extends LynxTemporalValue with HasProperty{
  def value: LocalDateTime = localDateTime

  def lynxType: LocalDateTimeType = LTLocalDateTime

  override def sameTypeCompareTo(o: LynxValue): Int = ???

  /*a mapping for time calculation */
  val timeUnit: Map[String, TemporalUnit] = Map(
    "years" -> ChronoUnit.YEARS, "months" -> ChronoUnit.MONTHS, "days" -> ChronoUnit.DAYS,
    "hours" -> ChronoUnit.HOURS, "minutes" -> ChronoUnit.MINUTES, "seconds" -> ChronoUnit.SECONDS,
    "milliseconds" -> ChronoUnit.MILLIS, "nanoseconds" -> ChronoUnit.NANOS
  )

  def plusDuration(that: LynxDuration): LynxLocalDateTime = {
    var aVal = localDateTime
    that.map.foreach(f => aVal = aVal.plus(f._2.toLong, timeUnit.get(f._1).get))
    LynxLocalDateTime(aVal)
  }

  def minusDuration(that: LynxDuration): LynxLocalDateTime = {
    var aVal = localDateTime
    that.map.foreach(f => aVal = aVal.minus(f._2.toLong, timeUnit.get(f._1).get))
    LynxLocalDateTime(aVal)
  }


  //LynxComponentDate
  val calendar = new GregorianCalendar()
  calendar.set(Calendar.YEAR, localDateTime.getYear)
  calendar.set(Calendar.DAY_OF_YEAR, localDateTime.getDayOfYear)
  var year: Int = localDateTime.getYear
  var month: Int = localDateTime.getMonthValue
  var quarter: Int = month - month % 3
  var week: Int = calendar.getWeeksInWeekYear
  var weekYear: Int = calendar.getWeekYear
  var ordinalDay: Int = localDateTime.getDayOfYear
  val calendar_quarterBegin = new GregorianCalendar()
  calendar_quarterBegin.set(Calendar.YEAR, localDateTime.getYear)
  calendar_quarterBegin.set(Calendar.MONTH, quarter)
  calendar_quarterBegin.set(Calendar.DAY_OF_MONTH, 1)
  var dayOfQuarter: Int = calendar.get(Calendar.DAY_OF_YEAR) - calendar_quarterBegin.get(Calendar.DAY_OF_YEAR) + 1
  var quarterDay: Int = calendar.get(Calendar.DAY_OF_YEAR) - calendar_quarterBegin.get(Calendar.DAY_OF_YEAR) + 1
  var day: Int = localDateTime.getDayOfMonth
  var dayOfWeek: Int = calendar.get(Calendar.DAY_OF_WEEK)
  var weekDay: Int = calendar.get(Calendar.DAY_OF_WEEK)

  //LynxComponentTime
  var hour: Int = localDateTime.getHour
  var minute: Int = localDateTime.getMinute
  var second: Int = localDateTime.getSecond
  var microsecond: Int = localDateTime.getNano * Math.pow(0.1, 6).toInt
  var millisecond: Int = (localDateTime.getNano * Math.pow(0.1, 3) % Math.pow(10, 3)).toInt
  var nanosecond: Int = localDateTime.getNano % Math.pow(10, 3).toInt
  var fraction: Int = localDateTime.getNano


  override def keys: Seq[LynxPropertyKey] = Seq("year", "quarter", "month", "week", "weekYear", "dayOfQuarter", "quarterDay", "day", "ordinalDay", "dayOfWeek", "weekDay", "hour", "minute", "second", "millisecond", "microsecond", "nanosecond").map(LynxPropertyKey)

  override def property(propertyKey: LynxPropertyKey): Option[LynxValue] = Some(propertyKey.value match {
    case "year" => LynxInteger(this.year)
    case "quarter" => LynxInteger(this.quarter)
    case "month" => LynxInteger(this.month)
    case "week" => LynxInteger(this.week)
    case "weekYear" => LynxInteger(this.weekYear)
    case "dayOfQuarter" => LynxInteger(this.dayOfQuarter)
    case "quarterDay" => LynxInteger(this.quarterDay)
    case "day" => LynxInteger(this.day)
    case "ordinalDay" => LynxInteger(this.ordinalDay)
    case "dayOfWeek" => LynxInteger(this.dayOfWeek)
    case "weekDay" => LynxInteger(this.weekDay)

    case "hour" => LynxInteger(this.hour)
    case "minute" => LynxInteger(this.minute)
    case "second" => LynxInteger(this.second)
    case "millisecond" => LynxInteger(this.millisecond)
    case "microsecond" => LynxInteger(this.microsecond + this.millisecond * Math.pow(10, 3).toLong)
    case "nanosecond" => LynxInteger(this.fraction)
    case _ => null
  })
}

object LynxLocalDateTime extends LynxTemporalParser {
  def now(): LynxLocalDateTime = LynxLocalDateTime(LocalDateTime.now())

  def now(zoneId: ZoneId): LynxLocalDateTime = LynxLocalDateTime(LocalDateTime.now(zoneId))

  def of(localDateTime: LocalDateTime): LynxLocalDateTime = LynxLocalDateTime(localDateTime)

  def of(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int, nanosecond: Int): LynxLocalDateTime =
    LynxLocalDateTime(LocalDateTime.of(year, month, day, hour, minute, second, nanosecond))

  def parse(localDateTimeStr: String): LynxLocalDateTime = {
    val map = splitDateTime(localDateTimeStr)
    val dateTuple = getYearMonthDay(map("dateStr"))
    val timeTuple = getHourMinuteSecond(map("timeStr"))

    val dateStr = dateTuple._1.formatted("%04d") + "-" + dateTuple._2.formatted("%02d") + "-" + dateTuple._3.formatted("%02d")
    val timeStr = timeTuple._1.formatted("%02d") + ":" + timeTuple._2.formatted("%02d") + ":" + timeTuple._3.formatted("%02d") + (timeTuple._4 match {
      case 0 => ""
      case v: Int => "." + v.formatted("%09d")
    })
    var dateTime = LocalDateTime.parse(dateStr + "T" + timeStr)
    LynxLocalDateTime(dateTime)
  }

  def parse(map: Map[String, Any]): LynxLocalDateTime = {
    if (map.isEmpty) {
      throw LynxTemporalParseException("At least one temporal unit must be specified")
    }
    var v: LocalDateTime = null
    if (map.contains("timezone")) {
      if (map.size == 1) {
        of(LocalDateTime.now(getZone(map("timezone").toString.replace(" ", "_"))))
      }
      else {
        throw LynxTemporalParseException("Cannot assign time zone if also assigning other fields")
      }
    }
    else if (map.contains("unitStr")) {
      val (truncate_year, truncate_month, truncate_day) = truncateDate(map)
      val (truncate_hour, truncate_minute, truncate_second, truncate_nanoOfSecond) = truncateTime(map)
      return of(truncate_year, truncate_month, truncate_day, truncate_hour, truncate_minute, truncate_second, truncate_nanoOfSecond)
    }

    else if (map.contains("year") || map.contains("date") || map.contains("datetime")) {
      val (year, month, day) = map match {
        case m if m.contains("dayOfWeek") => transformYearWeekDay(m)
        case m if m.contains("dayOfQuarter") => transformYearQuarterDay(m)
        case m if m.contains("ordinalDay") => transformYearOrdinalDay(m)
        case m if m.contains("date") => transformDate(m)
        case _ => getYearMonthDay(map)
      }
      val (hour: Int, minute: Int, second: Int) = map match {
        case m if m.contains("time") =>
          m("time") match {
            case v: LynxLocalTime => (v.hour, v.minute,
              if (m.contains("second")) m("second") match {
                case v: Long => v.toInt
                case v: LynxInteger => v.value.toInt
              }
              else v.second
            )
            case v: LynxTime => (v.hour, v.minute,
              if (m.contains("second")) m("second") match {
                case v: Long => v.toInt
                case v: LynxInteger => v.value.toInt
              }
              else v.second
            )
          }
        case _ => getHourMinuteSecond(map, requiredHasDay = false)
      }
      val nanoOfSecond: Int = map match {
        case m if m.contains("time") => (
          m.getOrElse("time", 0) match {
            case v: LocalTime => v.getNano
            case v: LynxInteger => v.value.toInt
            case LynxLocalTime(v) => v.getNano
            case LynxTime(v) => v.getNano
          })
        case _ => getNanosecond(map, requiredHasSecond = false)
      }
      of(year, month, day, hour, minute, second, nanoOfSecond)
    }
    else throw LynxTemporalParseException("parse date from map: map not contains (year, month, day) ")
  }
}
