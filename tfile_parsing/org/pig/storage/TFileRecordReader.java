package org.pig.storage;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.file.tfile.TFile;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class TFileRecordReader extends RecordReader<Text, Text> {

  private static final Log LOG = LogFactory.getLog(TFileRecordReader.class);

  private long start, end;

  private FSDataInputStream fin;
  private TFile.Reader reader;
  private TFile.Reader.Scanner scanner;

  private Text value = new Text();

  private BytesWritable bw = new BytesWritable();

  @Override public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    FileSplit fileSplit = (FileSplit) split;
    LOG.info("Initializing TFileRecordReader : " + fileSplit.getPath().toString());
    start = fileSplit.getStart();
    end = start + fileSplit.getLength();

    FileSystem fs = fileSplit.getPath().getFileSystem(context.getConfiguration());
    fin = fs.open(fileSplit.getPath());
    reader = new TFile.Reader(fin, fs.getFileStatus(fileSplit.getPath()).getLen(),
        context.getConfiguration());
    scanner = reader.createScannerByByteRange(start, fileSplit.getLength());
  }

  @Override public boolean nextKeyValue() throws IOException, InterruptedException {
    bw.setSize(0);
    if (!scanner.advance()) {
      value = null;
      return false;
    }
    scanner.entry().getValue(bw);
    value.set(bw.getBytes());
    return true;
  }

  @Override public Text getCurrentKey() throws IOException, InterruptedException {
    return null;
  }

  @Override public Text getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override public float getProgress() throws IOException, InterruptedException {
    return ((fin.getPos() - start) * 1.0f) / ((end - start) * 1.0f);
  }

  @Override public void close() throws IOException {
    IOUtils.closeQuietly(scanner);
    IOUtils.closeQuietly(reader);
    IOUtils.closeQuietly(fin);
  }
}