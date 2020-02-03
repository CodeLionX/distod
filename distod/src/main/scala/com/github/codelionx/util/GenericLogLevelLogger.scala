package com.github.codelionx.util

import org.slf4j.Logger


object GenericLogLevelLogger {

  implicit class ImplicitGenericLogLevelLogger(log: Logger) extends GenericLogLevelLogger(log)
}


class GenericLogLevelLogger(log: Logger) {

  def log(level: String, msg: String, args: Any*): Unit = level match {
    case "TRACE" => log.trace(msg, args: _*)
    case "DEBUG" => log.debug(msg, args: _*)
    case "INFO" => log.info(msg, args: _*)
    case "WARN" => log.warn(msg, args: _*)
    case "ERROR" => log.error(msg, args: _*)
    case "OFF" | _ =>
  }

  def isEnabled(level: String): Boolean = level match {
    case "TRACE" => log.isTraceEnabled()
    case "DEBUG" => log.isDebugEnabled()
    case "INFO" => log.isInfoEnabled()
    case "WARN" => log.isWarnEnabled()
    case "ERROR" => log.isErrorEnabled()
    case "OFF" | _ => false
  }
}
