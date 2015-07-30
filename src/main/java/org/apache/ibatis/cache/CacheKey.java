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
package org.apache.ibatis.cache;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Clinton Begin
 */
public class CacheKey implements Cloneable, Serializable { // 缓存键值对的键，构建CacheKey参考BaseExecutor.createCacheKey方法

  private static final long serialVersionUID = 1146682552656046210L;

  public static final CacheKey NULL_CACHE_KEY = new NullCacheKey(); // 表示一个没用的key单例 hama

  private static final int DEFAULT_MULTIPLYER = 37;
  private static final int DEFAULT_HASHCODE = 17;

  private int multiplier; // 默认就是DEFAULT_MULTIPLYER=37，也不变了
  private int hashcode; // hashcode，初始是17，计算看doUpdate方法
  private long checksum; // 校验和，计算看doUpdate方法
  private int count; // 「查询特征」的数量，应该与updateList.size()一致吧？ hama
  private List<Object> updateList; // 存「查询特征」，如statementId, rowBounds, 传递给JDBC的SQL, 传递给JDBC的参数值

  public CacheKey() { // 构造，初始化
    this.hashcode = DEFAULT_HASHCODE;
    this.multiplier = DEFAULT_MULTIPLYER;
    this.count = 0;
    this.updateList = new ArrayList<Object>();
  }

  public CacheKey(Object[] objects) { // 用一个Object数组构造
    this(); // 初始化
    updateAll(objects); //加入objects的所有元素到updateList
  }

  public int getUpdateCount() {
    return updateList.size();
  }

  public void update(Object object) { // 加入一个「查询特征」，存到updateList里
    if (object != null && object.getClass().isArray()) { // 若object是数组则doUpdate(每一个元素)
      int length = Array.getLength(object);
      for (int i = 0; i < length; i++) {
        Object element = Array.get(object, i);
        doUpdate(element);
      }
    } else { // 若object不是数组
      doUpdate(object);
    }
  }

  private void doUpdate(Object object) { // 加入一个「查询特征」的核心操作
    int baseHashCode = object == null ? 1 : object.hashCode(); // 空特征的baseHashCode是1

    count++; // 计数器，有几个特征
    checksum += baseHashCode; // 累加baseHashCode计算校验和
    baseHashCode *= count; // baseHashCode扩大count倍

    hashcode = multiplier * hashcode + baseHashCode; // 更新hashcode=默认乘子(37)*默认hashcode(17)+baseHashCode

    updateList.add(object); // 加入updateList
  }

  public void updateAll(Object[] objects) {
    for (Object o : objects) {
      update(o);
    }
  }

  @Override
  public boolean equals(Object object) { // 缓存判断命中时要用，hashcode checksum count均要相等 且 updateList每个元素equals
    if (this == object) {
      return true;
    }
    if (!(object instanceof CacheKey)) {
      return false;
    }

    final CacheKey cacheKey = (CacheKey) object;

    if (hashcode != cacheKey.hashcode) {
      return false;
    }
    if (checksum != cacheKey.checksum) {
      return false;
    }
    if (count != cacheKey.count) {
      return false;
    }

    for (int i = 0; i < updateList.size(); i++) { // updateList每个元素equals
      Object thisObject = updateList.get(i);
      Object thatObject = cacheKey.updateList.get(i);
      if (thisObject == null) {
        if (thatObject != null) {
          return false;
        }
      } else {
        if (!thisObject.equals(thatObject)) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    return hashcode;
  }

  @Override
  public String toString() {
    StringBuilder returnValue = new StringBuilder().append(hashcode).append(':').append(checksum);
    for (int i = 0; i < updateList.size(); i++) {
      returnValue.append(':').append(updateList.get(i));
    }

    return returnValue.toString();
  }

  @Override
  public CacheKey clone() throws CloneNotSupportedException {
    CacheKey clonedCacheKey = (CacheKey) super.clone();
    clonedCacheKey.updateList = new ArrayList<Object>(updateList); // 复制的CacheKey的updateList用新的引用
    return clonedCacheKey;
  }

}
