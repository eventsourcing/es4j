/**
 * Copyright (c) 2016, All Contributors (see CONTRIBUTORS file)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.eventsourcing.queries.options;

import com.googlecode.cqengine.query.option.QueryOptions;

/**
 * EagerFetching is a {@link QueryOptions} query option that signals to the indexing subsystem to eagerly fetch entities
 * if possible. Useful when iterating over the entire journal and processing the entire batch.
 */
public final class EagerFetching {
}
