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

import express from 'express';
import request from 'supertest';
import { createOpenApiRouter } from '../../schema/openapi/generated';
import { GetEventsModel } from './GetEvents.model';
import { bindGetEventsEndpoint } from './GetEvents.router';
import { parseCursor } from './GetEvents.utils';

jest.setTimeout(60_000);

describe('bindGetEventsEndpoint', () => {
  const model = {
    readEventsNonblocking: jest.fn(),
    blockUntilDataIsReady: jest.fn(),
  } satisfies GetEventsModel;
  let app: express.Express;

  beforeEach(async () => {
    jest.clearAllMocks();
    const router = await createOpenApiRouter();
    bindGetEventsEndpoint(router, model);
    app = express().use(router);
  });

  it('rejects illegal values', async () => {
    model.readEventsNonblocking.mockResolvedValueOnce({
      events: [],
      cursor: null,
    });
    await request(app)
      .get('/history/v1/events')
      .query({ limit: 1 })
      .expect(500); // should be 400, but this is what the generated router does for now
  });
});
