/**
 *    Copyright 2009-2015 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.cache.decorators;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.ibatis.cache.Cache;

/**
 * Soft Reference cache decorator
 * Thanks to Dr. Heinz Kabutz for his guidance here.
 *
 * @author Clinton Begin
 */
public class SoftCache implements Cache { // 内存空间不足的话软引用就会被回收
  private final Deque<Object> hardLinksToAvoidGarbageCollection; // 为了避免GC
  private final ReferenceQueue<Object> queueOfGarbageCollectedEntries; // 
  private final Cache delegate;
  private int numberOfHardLinks;

  public SoftCache(Cache delegate) {
    this.delegate = delegate;
    this.numberOfHardLinks = 256;
    this.hardLinksToAvoidGarbageCollection = new LinkedList<Object>();
    this.queueOfGarbageCollectedEntries = new ReferenceQueue<Object>();
  }

  @Override
  public String getId() {
    return delegate.getId();
  }

  @Override
  public int getSize() {
    removeGarbageCollectedItems(); // 删掉所有已被回收的缓存项
    return delegate.getSize();
  }


  public void setSize(int size) {
    this.numberOfHardLinks = size;
  }

  @Override
  public void putObject(Object key, Object value) {
    removeGarbageCollectedItems(); // 删掉所有已被回收的缓存项
    delegate.putObject(key, new SoftEntry(key, value, queueOfGarbageCollectedEntries)); // 加入的value是软引用的<k,v, queue>
  }
  // 笑断：假设不做这个列表，a函数和b函数，都从缓存里拿o，a用完了就不管了，这个时候o又变成软引用，随时可能被回收
  @Override // b函数过来拿，结果o没了，又重新创建，这个缓存就比较弱
  public Object getObject(Object key) { //hama
    Object result = null;
    @SuppressWarnings("unchecked") // assumed delegate cache is totally managed by this cache
    SoftReference<Object> softReference = (SoftReference<Object>) delegate.getObject(key);
    if (softReference != null) { // 检查是否已被回收
      result = softReference.get(); // get之前被回收的话，result也是null
      if (result == null) {
        delegate.removeObject(key);
      } else { // result非null，已经是个强引用了
        // See #586 (and #335) modifications need more than a read lock // hama
        synchronized (hardLinksToAvoidGarbageCollection) { // 保证list的线程安全
          hardLinksToAvoidGarbageCollection.addFirst(result); // 加入list防止被回收 hama
          if (hardLinksToAvoidGarbageCollection.size() > numberOfHardLinks) {
            hardLinksToAvoidGarbageCollection.removeLast();
          }
        }
      }
    }
    return result;
  }

  @Override
  public Object removeObject(Object key) {
    removeGarbageCollectedItems(); // 删掉所有已被回收的缓存项
    return delegate.removeObject(key);
  }

  @Override
  public void clear() {
    synchronized (hardLinksToAvoidGarbageCollection) { // 线程安全
      hardLinksToAvoidGarbageCollection.clear();
    }
    removeGarbageCollectedItems();
    delegate.clear();
  }

  @Override
  public ReadWriteLock getReadWriteLock() {
    return null;
  }

  private void removeGarbageCollectedItems() { // 弹出queueOfGarbageCollectedEntries的每一项，并删掉所有已被回收的缓存项
    SoftEntry sv;
    while ((sv = (SoftEntry) queueOfGarbageCollectedEntries.poll()) != null) { 
      delegate.removeObject(sv.key);
    }
  }

  private static class SoftEntry extends SoftReference<Object> {
    private final Object key;

    SoftEntry(Object key, Object value, ReferenceQueue<Object> garbageCollectionQueue) {
      super(value, garbageCollectionQueue); // 创建软引用，并把value注册到garbageCollectionQueue
      this.key = key;
    }
  }

}
