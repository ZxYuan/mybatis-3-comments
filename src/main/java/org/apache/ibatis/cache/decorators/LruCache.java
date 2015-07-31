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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.ibatis.cache.Cache;

/**
 * Lru (least recently used) cache decorator
 *
 * @author Clinton Begin
 */
public class LruCache implements Cache { // LRU最近最少使用算法的Cache装饰器

  private final Cache delegate; // 被装饰的Cache
  private Map<Object, Object> keyMap; // <key, key> 存缓存项的访问顺序
  private Object eldestKey;

  public LruCache(Cache delegate) {
    this.delegate = delegate;
    setSize(1024);
  }

  @Override
  public String getId() {
    return delegate.getId();
  }

  @Override
  public int getSize() {
    return delegate.getSize();
  }

  public void setSize(final int size) { // 初始化map
    keyMap = new LinkedHashMap<Object, Object>(size, .75F, true) { // 继承LinkedHashMap的匿名类, true指定access-order
      private static final long serialVersionUID = 4267176411845948333L;

      @Override // 默认返回false；在每次addEntry后判断这个方法是否为true，若为true则在map中删除eldest
      protected boolean removeEldestEntry(Map.Entry<Object, Object> eldest) { // override 移除最老项
        boolean tooBig = size() > size; // linkedMapHash的size大于缓存的指定size
        if (tooBig) {
          eldestKey = eldest.getKey(); // 保存目前最老项的key，之后该项在linkedHashMap中立即会被删除
        }
        return tooBig;
      }
    };
  }

  @Override
  public void putObject(Object key, Object value) { // 加入新的缓存项
    delegate.putObject(key, value);
    cycleKeyList(key); // 每次加入时调用一次
  }

  @Override
  public Object getObject(Object key) {
    keyMap.get(key); //touch // 改变access-order
    return delegate.getObject(key);
  }

  @Override
  public Object removeObject(Object key) {
    return delegate.removeObject(key);
  }

  @Override
  public void clear() {
    delegate.clear();
    keyMap.clear();
  }

  @Override
  public ReadWriteLock getReadWriteLock() {
    return null;
  }

  private void cycleKeyList(Object key) { // 每次加入新的缓存项后时被调用
    keyMap.put(key, key); // 往linkedHashMap里加入这个新缓存项的key
    if (eldestKey != null) { // 最老项非空，意味着有在linkedHashMap中被删了但在缓存中还未被删的具有同样key的项
      delegate.removeObject(eldestKey); // 从缓存中删除最老的缓存项
      eldestKey = null; // 没有剩余没被处理的已废弃缓存项了
    }
  }

}
