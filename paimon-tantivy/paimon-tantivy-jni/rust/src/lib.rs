// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

mod jni_directory;

use jni::objects::{JClass, JObject, JString, JValue};
use jni::sys::{jfloat, jint, jlong, jobject};
use jni::JNIEnv;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{Field, NumericOptions, Schema, Value, TEXT};
use tantivy::{Index, IndexReader, IndexWriter, ReloadPolicy};

use crate::jni_directory::JniDirectory;

/// Fixed schema: rowId (u64 fast field) + text (full-text indexed).
struct TantivyIndex {
    writer: IndexWriter,
    row_id_field: Field,
    text_field: Field,
}

struct TantivySearcherHandle {
    reader: IndexReader,
    row_id_field: Field,
    text_field: Field,
}

fn build_schema() -> (Schema, Field, Field) {
    let mut builder = Schema::builder();
    let row_id_field = builder.add_u64_field(
        "row_id",
        NumericOptions::default().set_stored().set_indexed(),
    );
    let text_field = builder.add_text_field("text", TEXT);
    (builder.build(), row_id_field, text_field)
}

// ---------------------------------------------------------------------------
// TantivyIndexWriter native methods
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_tantivy_TantivyIndexWriter_createIndex(
    mut env: JNIEnv,
    _class: JClass,
    index_path: JString,
) -> jlong {
    let path: String = env.get_string(&index_path).unwrap().into();
    let (schema, row_id_field, text_field) = build_schema();

    let dir = std::path::Path::new(&path);
    std::fs::create_dir_all(dir).unwrap();
    let index = Index::create_in_dir(dir, schema).unwrap();
    let writer = index.writer(50_000_000).unwrap();

    let handle = Box::new(TantivyIndex {
        writer,
        row_id_field,
        text_field,
    });
    Box::into_raw(handle) as jlong
}

#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_tantivy_TantivyIndexWriter_writeDocument(
    mut env: JNIEnv,
    _class: JClass,
    index_ptr: jlong,
    row_id: jlong,
    text: JString,
) {
    let handle = unsafe { &mut *(index_ptr as *mut TantivyIndex) };
    let text_str: String = env.get_string(&text).unwrap().into();

    let mut doc = tantivy::TantivyDocument::new();
    doc.add_u64(handle.row_id_field, row_id as u64);
    doc.add_text(handle.text_field, &text_str);
    handle.writer.add_document(doc).unwrap();
}

#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_tantivy_TantivyIndexWriter_commitIndex(
    _env: JNIEnv,
    _class: JClass,
    index_ptr: jlong,
) {
    let handle = unsafe { &mut *(index_ptr as *mut TantivyIndex) };
    handle.writer.commit().unwrap();
}

#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_tantivy_TantivyIndexWriter_freeIndex(
    _env: JNIEnv,
    _class: JClass,
    index_ptr: jlong,
) {
    unsafe {
        let _ = Box::from_raw(index_ptr as *mut TantivyIndex);
    }
}

// ---------------------------------------------------------------------------
// TantivySearcher native methods
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_tantivy_TantivySearcher_openIndex(
    mut env: JNIEnv,
    _class: JClass,
    index_path: JString,
) -> jlong {
    let path: String = env.get_string(&index_path).unwrap().into();
    let index = Index::open_in_dir(&path).unwrap();
    let schema = index.schema();

    let row_id_field = schema.get_field("row_id").unwrap();
    let text_field = schema.get_field("text").unwrap();

    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommitWithDelay)
        .try_into()
        .unwrap();

    let handle = Box::new(TantivySearcherHandle {
        reader,
        row_id_field,
        text_field,
    });
    Box::into_raw(handle) as jlong
}

/// Open an index from a Java StreamFileInput callback object.
///
/// fileNames: String[] — names of files in the archive
/// fileOffsets: long[] — byte offset of each file in the stream
/// fileLengths: long[] — byte length of each file
/// streamInput: StreamFileInput — Java object with seek(long) and read(byte[], int, int) methods
#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_tantivy_TantivySearcher_openFromStream(
    mut env: JNIEnv,
    _class: JClass,
    file_names: jni::objects::JObjectArray,
    file_offsets: jni::objects::JLongArray,
    file_lengths: jni::objects::JLongArray,
    stream_input: JObject,
) -> jlong {
    // Parse file metadata from Java arrays
    let count = env.get_array_length(&file_names).unwrap() as usize;
    let mut offsets_buf = vec![0i64; count];
    let mut lengths_buf = vec![0i64; count];
    env.get_long_array_region(&file_offsets, 0, &mut offsets_buf)
        .unwrap();
    env.get_long_array_region(&file_lengths, 0, &mut lengths_buf)
        .unwrap();

    let mut files = Vec::with_capacity(count);
    for i in 0..count {
        let obj = env
            .get_object_array_element(&file_names, i as i32)
            .unwrap();
        let jstr = JString::from(obj);
        let name: String = env.get_string(&jstr).unwrap().into();
        files.push((name, offsets_buf[i] as u64, lengths_buf[i] as u64));
    }

    // Create a global ref to the Java stream callback
    let jvm = env.get_java_vm().unwrap();
    let stream_ref = env.new_global_ref(stream_input).unwrap();

    let directory = JniDirectory::new(jvm, stream_ref, files);
    let index = Index::open(directory).unwrap();
    let schema = index.schema();

    let row_id_field = schema.get_field("row_id").unwrap();
    let text_field = schema.get_field("text").unwrap();

    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()
        .unwrap();

    let handle = Box::new(TantivySearcherHandle {
        reader,
        row_id_field,
        text_field,
    });
    Box::into_raw(handle) as jlong
}

/// Search and return a SearchResult(long[] rowIds, float[] scores).
#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_tantivy_TantivySearcher_searchIndex(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    query_string: JString,
    limit: jint,
) -> jobject {
    let handle = unsafe { &*(searcher_ptr as *const TantivySearcherHandle) };
    let query_str: String = env.get_string(&query_string).unwrap().into();

    let searcher = handle.reader.searcher();
    let query_parser = QueryParser::for_index(&searcher.index(), vec![handle.text_field]);
    let query = query_parser.parse_query(&query_str).unwrap();
    let top_docs = searcher
        .search(&query, &TopDocs::with_limit(limit as usize))
        .unwrap();

    let count = top_docs.len();

    // Build Java long[] and float[]
    let row_id_array = env.new_long_array(count as i32).unwrap();
    let score_array = env.new_float_array(count as i32).unwrap();

    let mut row_ids: Vec<jlong> = Vec::with_capacity(count);
    let mut scores: Vec<jfloat> = Vec::with_capacity(count);

    for (score, doc_address) in &top_docs {
        let doc: tantivy::TantivyDocument = searcher.doc(*doc_address).unwrap();
        let row_id = doc
            .get_first(handle.row_id_field)
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as jlong;
        row_ids.push(row_id);
        scores.push(*score as jfloat);
    }

    env.set_long_array_region(&row_id_array, 0, &row_ids)
        .unwrap();
    env.set_float_array_region(&score_array, 0, &scores)
        .unwrap();

    // Construct SearchResult object
    let class = env
        .find_class("org/apache/paimon/tantivy/SearchResult")
        .unwrap();
    let obj = env
        .new_object(
            class,
            "([J[F)V",
            &[
                JValue::Object(&JObject::from(row_id_array)),
                JValue::Object(&JObject::from(score_array)),
            ],
        )
        .unwrap();

    obj.into_raw()
}

#[no_mangle]
pub extern "system" fn Java_org_apache_paimon_tantivy_TantivySearcher_freeSearcher(
    _env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
) {
    unsafe {
        let _ = Box::from_raw(searcher_ptr as *mut TantivySearcherHandle);
    }
}
