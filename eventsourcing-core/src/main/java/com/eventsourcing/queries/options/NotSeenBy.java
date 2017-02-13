/**
 * Copyright (c) 2016, All Contributors (see CONTRIBUTORS file)
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.eventsourcing.queries.options;

import com.googlecode.cqengine.query.option.QueryOptions;
import lombok.Value;

/**
 * This {@link QueryOptions} query option allows to optimize
 * iterating over the journal in special circumstances, like an index
 * that doesn't want to reindex what already has been indexed.
 */
@Value
public class NotSeenBy {
    byte[] id;
}
