
package datafu.pig.udf_kmeans;

import java.io.IOException;

import org.apache.pig.Accumulator;
import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.joda.time.DateTime;
import org.joda.time.Period;

import datafu.pig.util.SimpleEvalFunc;

/**
 * Performs a count of events, ignoring events which occur within the
 * same time window.
 * <p>
 * This is useful for tasks such as counting the number of page views per user since it:
 *  a) prevent reloads and go-backs from overcounting actual views
 *  b) captures the notion that views across multiple sessions are more meaningful
 * <p>
 * Input <b>must</b> be sorted ascendingly by time for this UDF to work.
 * <p>
 * Example:
 * <pre>
 * {@code
 * 
 * %declare TIME_WINDOW  10m
 * 
 * define SessionCount datafu.pig.sessions.SessionCount('$TIME_WINDOW');
 * 
 * views = LOAD 'views' as (user_id:int, page_id:int, time:chararray);
 * views_grouped = GROUP views by (user_id, page_id);
 * view_counts = FOREACH views_grouped { 
 *   views = order views by time;
 *   generate group.user_id as user_id, 
 *            group.page_id as page_id, 
 *            SessionCount(views.(time)) as count; }
 * }
 * </pre>
 * 
 */
public class KmeansLatLonUdf extends AccumulatorEvalFunc<Long>
{
  private final long millis;
  private DateTime last_date;
  private long sum;

  public KmeansLatLonUdf()
  {
    cleanup();
  }

  @Override
  public void accumulate(Tuple input) throws IOException
  {
    for (Tuple t : (DataBag) input.get(0)) {
      DateTime date = new DateTime(t.get(0));

      if (last_date == null) {
        last_date = date;
        sum = 1;
      } else if (date.isAfter(last_date.plus(this.millis)))
        sum += 1;
      else if (date.isBefore(last_date))
        throw new IOException("input time series is not sorted");

      last_date = date;
    }    
  }

  @Override
  public Long getValue()
  {
    return sum;
  }

  @Override
  public void cleanup()
  {
  }
}
