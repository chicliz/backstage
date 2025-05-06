/*
 * Copyright 2025 The Backstage Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { durationToMilliseconds, HumanDuration } from '@backstage/types';
import { Knex } from 'knex';
import { SubscriptionEvent } from '../../consumers';
import {
  readEventsTableRows,
  ReadEventsTableRowsOptions,
} from '../../database/readEventsTableRows';
import { Cursor } from './GetEvents.utils';
import { getMaxId } from '../../database/getMaxId';

export interface GetEventsModel {
  readEventsNonblocking(options: {
    readOptions: ReadEventsTableRowsOptions;
    block: boolean;
  }): Promise<{ events: SubscriptionEvent[]; cursor?: Cursor }>;
  blockUntilDataIsReady(options: {
    readOptions: ReadEventsTableRowsOptions;
  }): Promise<void>;
}

export class GetEventsModelImpl implements GetEventsModel {
  #knexPromise: Promise<Knex>;
  #signal: AbortSignal;
  #blockDurationMillis: number;
  #blockPollFrequencyMillis: number;

  constructor(options: {
    knexPromise: Promise<Knex>;
    signal: AbortSignal;
    blockDuration?: HumanDuration;
    blockPollFrequency?: HumanDuration;
  }) {
    this.#knexPromise = options.knexPromise;
    this.#signal = options.signal;
    this.#blockDurationMillis = durationToMilliseconds(
      options.blockDuration ?? { seconds: 10 },
    );
    this.#blockPollFrequencyMillis = durationToMilliseconds(
      options.blockPollFrequency ?? { seconds: 1 },
    );
  }

  async readEventsNonblocking(options: {
    readOptions: ReadEventsTableRowsOptions;
    block: boolean;
  }): Promise<{ events: SubscriptionEvent[]; cursor?: Cursor }> {
    const knex = await this.#knexPromise;

    let readOptions = options.readOptions;
    if (readOptions.afterEventId === 'last') {
      readOptions = { ...readOptions, afterEventId: await getMaxId(knex) };
    }

    const events = await readEventsTableRows(knex, readOptions);

    // Let's generate a cursor for continuing to read, if we got some rows OR if
    // we were reading in ascending order (because then there might be more
    // events next time around)
    const shouldReturnCursor = events.length > 0 || readOptions.order === 'asc';
    let cursor: Cursor | undefined;
    if (shouldReturnCursor) {
      cursor = {
        version: 1,
        afterEventId:
          events.length > 0
            ? events[events.length - 1].id
            : readOptions.afterEventId,
        entityRef: readOptions.entityRef,
        entityId: readOptions.entityId,
        order: readOptions.order,
        limit: readOptions.limit,
        block: options.block,
      };
    }

    return { events, cursor };
  }

  // TODO(freben): Implement a more efficient way to wait for new events.
  // See the events backend using LISTEN/NOTIFY for inspiration. For now, wait
  // for up to 10 seconds and stop early if the request closes, or if we are
  // shutting down, or we start finding some rows.
  async blockUntilDataIsReady(options: {
    readOptions: ReadEventsTableRowsOptions;
  }): Promise<void> {
    const knex = await this.#knexPromise;
    const deadline = Date.now() + this.#blockDurationMillis;
    const signal = this.#signal;

    while (Date.now() < deadline) {
      await new Promise<void>(resolve => {
        const timer = setTimeout(done, this.#blockPollFrequencyMillis);
        signal.addEventListener('abort', done);
        function done() {
          clearTimeout(timer);
          signal.removeEventListener('abort', done);
          resolve();
        }
      });
      if (
        this.#signal.aborted ||
        (
          await readEventsTableRows(knex, {
            ...options.readOptions,
            limit: 1,
          })
        ).length
      ) {
        break;
      }
    }
  }
}
