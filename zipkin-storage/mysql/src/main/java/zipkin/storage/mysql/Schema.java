/**
 * Copyright 2015-2016 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin.storage.mysql;

import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import javax.sql.DataSource;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Row2;
import org.jooq.SelectOffsetStep;
import zipkin.internal.Lazy;
import zipkin.internal.Pair;
import zipkin.storage.mysql.internal.generated.tables.ZipkinAnnotations;

import static org.jooq.impl.DSL.row;
import static zipkin.storage.mysql.internal.generated.tables.ZipkinAnnotations.ZIPKIN_ANNOTATIONS;
import static zipkin.storage.mysql.internal.generated.tables.ZipkinSpans.ZIPKIN_SPANS;

@AutoValue
abstract class Schema {

  abstract List<Field<?>> spanIdFields();

  abstract List<Field<?>> spanFields();

  abstract List<Field<?>> annotationFields();

  abstract List<Field<?>> dependencyLinkFields();

  abstract List<Field<?>> dependencyLinkGroupByFields();

  abstract boolean hasTraceIdHigh();

  abstract boolean hasPreAggregatedDependencies();

  abstract boolean hasIpv6();

  Condition joinCondition(ZipkinAnnotations annotationTable) {
    if (hasTraceIdHigh()) {
      return ZIPKIN_SPANS.TRACE_ID_HIGH.eq(annotationTable.TRACE_ID_HIGH)
          .and(ZIPKIN_SPANS.TRACE_ID.eq(annotationTable.TRACE_ID))
          .and(ZIPKIN_SPANS.ID.eq(annotationTable.SPAN_ID));
    } else {
      return ZIPKIN_SPANS.TRACE_ID.eq(annotationTable.TRACE_ID)
          .and(ZIPKIN_SPANS.ID.eq(annotationTable.SPAN_ID));
    }
  }

  static Lazy<Schema> parse(final DataSource datasource, final DSLContexts context) {
    return new Lazy<Schema>() {
      @Override protected Schema compute() {
        boolean hasTraceIdHigh = new HasTraceIdHigh(datasource, context).get();
        boolean hasPreAggregatedDependencies =
            new HasPreAggregatedDependencies(datasource, context).get();
        boolean hasIpv6 = new HasIpv6(datasource, context).get();

        List<Field<?>> spanIdFields = list(ZIPKIN_SPANS.TRACE_ID_HIGH, ZIPKIN_SPANS.TRACE_ID);
        List<Field<?>> spanFields = list(ZIPKIN_SPANS.fields());
        List<Field<?>> annotationFields = list(ZIPKIN_ANNOTATIONS.fields());
        List<Field<?>> dependencyLinkFields = list(
            ZIPKIN_SPANS.TRACE_ID_HIGH, ZIPKIN_SPANS.TRACE_ID, ZIPKIN_SPANS.PARENT_ID,
            ZIPKIN_SPANS.ID, ZIPKIN_ANNOTATIONS.A_KEY, ZIPKIN_ANNOTATIONS.ENDPOINT_SERVICE_NAME
        );
        List<Field<?>> dependencyLinkGroupByFields = new ArrayList<>(dependencyLinkFields);
        dependencyLinkGroupByFields.remove(ZIPKIN_SPANS.PARENT_ID);
        if (!hasTraceIdHigh) {
          spanIdFields.remove(ZIPKIN_SPANS.TRACE_ID_HIGH);
          spanFields.remove(ZIPKIN_SPANS.TRACE_ID_HIGH);
          annotationFields.remove(ZIPKIN_ANNOTATIONS.TRACE_ID_HIGH);
          dependencyLinkFields.remove(ZIPKIN_SPANS.TRACE_ID_HIGH);
          dependencyLinkGroupByFields.remove(ZIPKIN_SPANS.TRACE_ID_HIGH);
        }
        if (!hasIpv6) {
          annotationFields.remove(ZIPKIN_ANNOTATIONS.ENDPOINT_IPV6);
        }

        return new AutoValue_Schema(
            spanIdFields,
            spanFields,
            annotationFields,
            dependencyLinkFields,
            dependencyLinkGroupByFields,
            hasTraceIdHigh,
            hasPreAggregatedDependencies,
            hasIpv6
        );
      }
    };
  }

  /** Returns a mutable list */
  static <T> List<T> list(T... elements) {
    return new ArrayList<T>(Arrays.asList(elements));
  }

  Condition spanTraceIdCondition(SelectOffsetStep<? extends Record> traceIdQuery) {
    if (hasTraceIdHigh()) {
      Result<? extends Record> result = traceIdQuery.fetch();
      List<Row2<Long, Long>> traceIds = new ArrayList<>(result.size());
      for (Record r : result) {
        traceIds.add(row(r.get(ZIPKIN_SPANS.TRACE_ID_HIGH), r.get(ZIPKIN_SPANS.TRACE_ID)));
      }
      return row(ZIPKIN_SPANS.TRACE_ID_HIGH, ZIPKIN_SPANS.TRACE_ID).in(traceIds);
    } else {
      List<Long> traceIds = traceIdQuery.fetch(ZIPKIN_SPANS.TRACE_ID);
      return ZIPKIN_SPANS.TRACE_ID.in(traceIds);
    }
  }

  Condition spanTraceIdCondition(Long traceIdHigh, long traceIdLow) {
    return traceIdHigh != null && hasTraceIdHigh()
        ? row(ZIPKIN_SPANS.TRACE_ID_HIGH, ZIPKIN_SPANS.TRACE_ID).eq(traceIdHigh, traceIdLow)
        : ZIPKIN_SPANS.TRACE_ID.eq(traceIdLow);
  }

  Condition annotationsTraceIdCondition(Set<Pair<Long>> traceIds) {
    boolean hasTraceIdHigh = false;
    for (Pair<Long> traceId : traceIds) {
      if (traceId._1 != 0) {
        hasTraceIdHigh = true;
        break;
      }
    }
    if (hasTraceIdHigh) {
      Row2[] result = new Row2[traceIds.size()];
      int i = 0;
      for (Pair<Long> traceId128 : traceIds) {
        result[i++] = row(traceId128._1, traceId128._2);
      }
      return row(ZIPKIN_ANNOTATIONS.TRACE_ID_HIGH, ZIPKIN_ANNOTATIONS.TRACE_ID).in(result);
    } else {
      Long[] result = new Long[traceIds.size()];
      int i = 0;
      for (Pair<Long> traceId128 : traceIds) {
        result[i++] = traceId128._2;
      }
      return ZIPKIN_ANNOTATIONS.TRACE_ID.in(result);
    }
  }
}
