/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.weakref.s3fs.util;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;


class S3OutputStream { /* extends OutputStream {
  
  private int bufferSize;

  private Path path;

  private long blockSize;

  private File backupFile;

  private OutputStream backupStream;

  private Random r = new Random();

  private boolean closed;

  private int pos = 0;

  private long filePos = 0;

  private int bytesWrittenToBlock = 0;

  private byte[] outBuf;

  private List<Block> blocks = new ArrayList<Block>();

  private Block nextBlock;
  
  private static final Log LOG = 
    LogFactory.getLog(S3OutputStream.class.getName());


  public S3OutputStream(long blockSize,
                        int buffersize) throws IOException {
    
    this.path = path;
    this.blockSize = blockSize;
    this.backupFile = newBackupFile();
    this.backupStream = new FileOutputStream(backupFile);
    this.bufferSize = buffersize;
    this.outBuf = new byte[bufferSize];

  }

  private File newBackupFile() throws IOException {
    File dir = new File(conf.get("fs.s3.buffer.dir"));
    if (!dir.exists() && !dir.mkdirs()) {
      throw new IOException("Cannot create S3 buffer directory: " + dir);
    }
    File result = File.createTempFile("output-", ".tmp", dir);
    result.deleteOnExit();
    return result;
  }

  public long getPos() throws IOException {
    return filePos;
  }

  @Override
  public synchronized void write(int b) throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }

    if ((bytesWrittenToBlock + pos == blockSize) || (pos >= bufferSize)) {
      flush();
    }
    outBuf[pos++] = (byte) b;
    filePos++;
  }

  @Override
  public synchronized void write(byte b[], int off, int len) throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
    while (len > 0) {
      int remaining = bufferSize - pos;
      int toWrite = Math.min(remaining, len);
      System.arraycopy(b, off, outBuf, pos, toWrite);
      pos += toWrite;
      off += toWrite;
      len -= toWrite;
      filePos += toWrite;

      if ((bytesWrittenToBlock + pos >= blockSize) || (pos == bufferSize)) {
        flush();
      }
    }
  }

  @Override
  public synchronized void flush() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }

    if (bytesWrittenToBlock + pos >= blockSize) {
      flushData((int) blockSize - bytesWrittenToBlock);
    }
    if (bytesWrittenToBlock == blockSize) {
      endBlock();
    }
    flushData(pos);
  }

  private synchronized void flushData(int maxPos) throws IOException {
    int workingPos = Math.min(pos, maxPos);

    if (workingPos > 0) {
      //
      // To the local block backup, write just the bytes
      //
      backupStream.write(outBuf, 0, workingPos);

      //
      // Track position
      //
      bytesWrittenToBlock += workingPos;
      System.arraycopy(outBuf, workingPos, outBuf, 0, pos - workingPos);
      pos -= workingPos;
    }
  }

  private synchronized void endBlock() throws IOException {
    //
    // Done with local copy
    //
    backupStream.close();

    //
    // Send it to S3
    //
    // TODO: Use passed in Progressable to report progress.
    nextBlockOutputStream();
    storeBlock(nextBlock, backupFile);
    internalClose();

    //
    // Delete local backup, start new one
    //
    boolean b = backupFile.delete();
    if (!b) {
      LOG.warn("Ignoring failed delete");
    }
    backupFile = newBackupFile();
    backupStream = new FileOutputStream(backupFile);
    bytesWrittenToBlock = 0;
  }

  private synchronized void nextBlockOutputStream() throws IOException {
    long blockId = r.nextLong();
    while (store.blockExists(blockId)) {
      blockId = r.nextLong();
    }
    nextBlock = new Block(blockId, bytesWrittenToBlock);
    blocks.add(nextBlock);
    bytesWrittenToBlock = 0;
  }

  private synchronized void internalClose() throws IOException {
    INode inode = new INode(FileType.FILE, blocks.toArray(new Block[blocks
                                                                    .size()]));
    store.storeINode(path, inode);
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }

    flush();
    if (filePos == 0 || bytesWrittenToBlock != 0) {
      endBlock();
    }

    backupStream.close();
    boolean b = backupFile.delete();
    if (!b) {
      LOG.warn("Ignoring failed delete");
    }

    super.close();

    closed = true;
  }
  
  public void storeBlock(Block block, File file) throws IOException {
	    BufferedInputStream in = null;
	    try {
	      in = new BufferedInputStream(new FileInputStream(file));
	      put(blockToKey(block), in, block.getLength(), false);
	    } finally {
	      closeQuietly(in);
	    }    
	  }
  
  public boolean blockExists(long blockId) throws IOException {
	    InputStream in = get(blockToKey(blockId), false);
	    if (in == null) {
	      return false;
	    }
	    in.close();
	    return true;
	  }
  
  private InputStream get(String key, boolean checkMetadata)
	      throws IOException {
	    
	    try {
	      S3Object object = s3Service.getObject(bucket, key);
	      if (checkMetadata) {
	        checkMetadata(object);
	      }
	      return object.getDataInputStream();
	    } catch (S3ServiceException e) {
	      if ("NoSuchKey".equals(e.getS3ErrorCode())) {
	        return null;
	      }
	      if (e.getCause() instanceof IOException) {
	        throw (IOException) e.getCause();
	      }
	      throw new S3Exception(e);
	    }
	  }
  
  private void put(String key, InputStream in, long length, boolean storeMetadata)
	      throws IOException {
	    
	    try {
	    	ObjectMetadata metadata = new ObjectMetadata();
	    	metadata.setContentType("binary/octet-stream");
	    	metadata.setContentLength(length);
	      S3Object object = new S3Object();
	      object.setKey(key);
	      object.setObjectContent(in);
	      object.setObjectMetadata(metadata);
	      if (storeMetadata) {
	        object.addAllMetadata(METADATA);
	      }
	      s3Service.putObject(bucket, object);
	    } catch (S3ServiceException e) {
	      if (e.getCause() instanceof IOException) {
	        throw (IOException) e.getCause();
	      }
	      throw new S3Exception(e);
	    }
	  }
  
  public class Block {
	  private long id;

	  private long length;

	  public Block(long id, long length) {
	    this.id = id;
	    this.length = length;
	  }

	  public long getId() {
	    return id;
	  }

	  public long getLength() {
	    return length;
	  }

	  @Override
	  public String toString() {
	    return "Block[" + id + ", " + length + "]";
	  }

	}
*/
}