package org.mrgeo.utils.logging;

import org.apache.log4j.Appender;
import org.apache.log4j.Layout;
import org.apache.log4j.spi.ErrorHandler;
import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Log4JAppenderWrapper implements Appender
{
final Appender wrappedAppender;
final Pattern crlf = Pattern.compile("\r\n|\n|\r");

public Log4JAppenderWrapper(Appender wrappedAppender)
{
  this.wrappedAppender = wrappedAppender;
}

@Override
public void addFilter(Filter newFilter)
{
  assert (wrappedAppender != null);
  wrappedAppender.addFilter(newFilter);
}

@Override
public Filter getFilter()
{
  assert (wrappedAppender != null);
  return wrappedAppender.getFilter();
}

@Override
public void clearFilters()
{
  assert (wrappedAppender != null);
  wrappedAppender.clearFilters();
}

@Override
public void close()
{
  assert (wrappedAppender != null);
  wrappedAppender.close();
}

@Override
public void doAppend(LoggingEvent event)
{
  assert (wrappedAppender != null);


  final String msg = event.getMessage().toString();

  Matcher m = crlf.matcher(msg);
  if (m.find())
  {
    String lines[] = msg.split("\r\n|\n|\r"); //msg.split("\\r?\\n");

    for (String line : lines)
    {
      String clean = "(Encoded) " + line;

      LoggingEvent encoded = new LoggingEvent(event.getFQNOfLoggerClass(),
          event.getLogger(), event.getTimeStamp(), event.getLevel(), clean,
          event.getThreadName(), event.getThrowableInformation(), event.getNDC(),
          event.getLocationInformation(), event.getProperties());

      wrappedAppender.doAppend(encoded);
    }
  }
  else
  {
    wrappedAppender.doAppend(event);
  }

}

@Override
public String getName()
{
  assert (wrappedAppender != null);
  return "(wrapped) " + wrappedAppender.getName();
}

@Override
public void setName(String name)
{
  assert (wrappedAppender != null);
  wrappedAppender.setName(name);
}

@Override
public ErrorHandler getErrorHandler()
{
  assert (wrappedAppender != null);
  return wrappedAppender.getErrorHandler();
}

@Override
public void setErrorHandler(ErrorHandler errorHandler)
{
  assert (wrappedAppender != null);
  wrappedAppender.setErrorHandler(errorHandler);
}

@Override
public Layout getLayout()
{
  assert (wrappedAppender != null);
  return wrappedAppender.getLayout();
}

@Override
public void setLayout(Layout layout)
{
  assert (wrappedAppender != null);
  wrappedAppender.setLayout(layout);
}

@Override
public boolean requiresLayout()
{
  assert (wrappedAppender != null);
  return wrappedAppender.requiresLayout();
}
}
