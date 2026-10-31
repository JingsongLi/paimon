# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import json

import pytest

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.common.options.vector_index_options import VectorIndexOptions


def _assert_metric(options, expected):
    for key in ('ivf-pq.metric', 'ivf-pq.distance.metric',
                'fields.embedding.metric', 'fields.embedding.distance.metric'):
        assert options.to_map()[key] == expected


@pytest.mark.parametrize('key,value,expected', [
    ('fields.embedding.pk-vector.distance.metric', 'COSINE', 'cosine'),
    ('fields.embedding.distance.metric', 'COSINE', 'cosine'),
    ('fields.embedding.metric', 'INNER-PRODUCT', 'inner_product'),
    ('ivf-pq.distance.metric', 'COSINE', 'cosine'),
    ('ivf-pq.metric', 'L2', 'l2'),
    ('distance.metric', 'INNER-PRODUCT', 'inner_product'),
    ('metric', 'COSINE', 'cosine'),
])
def test_legacy_and_dlf_metric_aliases(key, value, expected):
    values = {'global-index.ivf-pq.index-column': 'embedding', 'ivf-pq.dimension': '768',
              'fields.embedding.nlist': '48', key: value}
    resolved = VectorIndexOptions.resolve(Options(values), None, 'embedding')
    _assert_metric(resolved, expected)
    assert resolved.to_map()['ivf-pq.dimension'] == '768'
    assert resolved.to_map()['fields.embedding.nlist'] == '48'
    assert VectorIndexOptions.resolve(resolved, 'ivf-pq', 'embedding').to_map() == resolved.to_map()
    assert values[key] == value


@pytest.mark.parametrize('suffix,key', [
    ('index-options', 'metric'), ('index-options', 'distance.metric'),
    ('pk-vector.index.options', 'metric'),
])
def test_field_json_aliases_remain_idempotent(suffix, key):
    values = {'pk-vector.index.columns': 'embedding',
              'fields.embedding.pk-vector.index.type': 'ivf-pq',
              'fields.embedding.pk-vector.distance.metric': 'cosine',
              'fields.embedding.' + suffix: json.dumps({key: 'COSINE', 'nlist': 64})}
    resolved = VectorIndexOptions.resolve(Options(values), None, 'embedding')
    _assert_metric(resolved, 'cosine')
    assert resolved.to_map()['ivf-pq.nlist'] == '64'
    assert VectorIndexOptions.resolve(resolved, 'ivf-pq', 'embedding').to_map() == resolved.to_map()
    assert CoreOptions(Options(values)).primary_key_vector_index_options('embedding').to_map() == resolved.to_map()


def test_reader_keeps_unspecified_metric_and_writer_uses_default():
    values = Options({'fields.embedding.index-options': '{"nlist":64}'})
    resolved = VectorIndexOptions.resolve(values, 'ivf-pq', 'embedding', default_metric=False).to_map()
    assert resolved['ivf-pq.nlist'] == '64'
    assert 'ivf-pq.metric' not in resolved
    assert 'fields.embedding.distance.metric' not in resolved
    _assert_metric(VectorIndexOptions.resolve(values, 'ivf-pq', 'embedding'), 'inner_product')
    assert VectorIndexOptions.resolve(values, None, 'embedding').to_map() == values.to_map()
