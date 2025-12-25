package org.apache.calcite.adapter.restapi.rest.interfaces;

import org.apache.calcite.adapter.restapi.rest.ResponseRowReader;

import java.util.List;

/**
 * Interface for paging data from a REST API source in Calcite integrations.
 * <p>
 * Implementations manage retrieval of additional rows, batches, or "pages" from
 * the remote REST endpoint. Used for efficient streaming or paginated results.
 * </p>
 */
public interface RestIterator {

    /**
     * Fetches the next batch/page of rows from the REST source.
     *
     * @return a list of {@link ResponseRowReader} representing new rows.
     *         Returns an empty list when no further data is available.
     */
    List<ResponseRowReader> getMore();

}
