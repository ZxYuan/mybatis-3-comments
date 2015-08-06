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
package org.apache.ibatis.session.defaults;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ibatis.binding.BindingException;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.exceptions.ExceptionFactory;
import org.apache.ibatis.exceptions.TooManyResultsException;
import org.apache.ibatis.executor.BatchResult;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.executor.result.DefaultMapResultHandler;
import org.apache.ibatis.executor.result.DefaultResultContext;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.session.SqlSession;

/**
 *
 * The default implementation for {@link SqlSession}.
 * Note that this class is not Thread-Safe.
 *
 * @author Clinton Begin
 */
public class DefaultSqlSession implements SqlSession { // 默认sqlSession

  private Configuration configuration; // mybatis的configuration
  private Executor executor; // 用于真正执行crud等操作

  private boolean autoCommit; // 标识是否自动提交
  private boolean dirty; // hama
  private List<Cursor<?>> cursorList; // 游标list

  public DefaultSqlSession(Configuration configuration, Executor executor, boolean autoCommit) {
    this.configuration = configuration;
    this.executor = executor;
    this.dirty = false;
    this.autoCommit = autoCommit;
  }

  public DefaultSqlSession(Configuration configuration, Executor executor) {
    this(configuration, executor, false);
  }

  @Override
  public <T> T selectOne(String statement) {
    return this.<T>selectOne(statement, null);
  }

  @Override
  public <T> T selectOne(String statement, Object parameter) { // 返回一条记录
    // Popular vote was to return null on 0 results and throw exception on too many.
    List<T> list = this.<T>selectList(statement, parameter); // 先取出一个list
    if (list.size() == 1) {
      return list.get(0); // 再返回第一个元素
    } else if (list.size() > 1) {
      throw new TooManyResultsException("Expected one result (or null) to be returned by selectOne(), but found: " + list.size());
    } else {
      return null;
    }
  }

  @Override
  public <K, V> Map<K, V> selectMap(String statement, String mapKey) {
    return this.selectMap(statement, null, mapKey, RowBounds.DEFAULT);
  }

  @Override
  public <K, V> Map<K, V> selectMap(String statement, Object parameter, String mapKey) {
    return this.selectMap(statement, parameter, mapKey, RowBounds.DEFAULT);
  }

  @Override
  public <K, V> Map<K, V> selectMap(String statement, Object parameter, String mapKey, RowBounds rowBounds) { // hama
    final List<? extends V> list = selectList(statement, parameter, rowBounds); // 取出一个list
    final DefaultMapResultHandler<K, V> mapResultHandler = new DefaultMapResultHandler<K, V>(mapKey, // hama
        configuration.getObjectFactory(), configuration.getObjectWrapperFactory(), configuration.getReflectorFactory());
    final DefaultResultContext<V> context = new DefaultResultContext<V>();
    for (V o : list) {
      context.nextResultObject(o);
      mapResultHandler.handleResult(context);
    }
    return mapResultHandler.getMappedResults();
  }

  @Override
  public <T> Cursor<T> selectCursor(String statement) {
    return selectCursor(statement, null);
  }

  @Override
  public <T> Cursor<T> selectCursor(String statement, Object parameter) {
    return selectCursor(statement, parameter, RowBounds.DEFAULT);
  }

  @Override
  public <T> Cursor<T> selectCursor(String statement, Object parameter, RowBounds rowBounds) { // 获得游标
    try {
      MappedStatement ms = configuration.getMappedStatement(statement); // 获得MappedStatment
      Cursor<T> cursor = executor.queryCursor(ms, wrapCollection(parameter), rowBounds); // 用executor执行查询
      registerCursor(cursor); // 注册游标 hama
      return cursor;
    } catch (Exception e) {
      throw ExceptionFactory.wrapException("Error querying database.  Cause: " + e, e);
    } finally {
      ErrorContext.instance().reset(); // hama
    }
  }

  @Override
  public <E> List<E> selectList(String statement) {
    return this.selectList(statement, null);
  }

  @Override
  public <E> List<E> selectList(String statement, Object parameter) {
    return this.selectList(statement, parameter, RowBounds.DEFAULT);
  }

  @Override
  public <E> List<E> selectList(String statement, Object parameter, RowBounds rowBounds) { // 取出list
    try {
      MappedStatement ms = configuration.getMappedStatement(statement); // 获得MappedStatement
      return executor.query(ms, wrapCollection(parameter), rowBounds, Executor.NO_RESULT_HANDLER); // 用executor查询
    } catch (Exception e) {
      throw ExceptionFactory.wrapException("Error querying database.  Cause: " + e, e);
    } finally {
      ErrorContext.instance().reset(); // hama
    }
  }

  @Override
  public void select(String statement, Object parameter, ResultHandler handler) {
    select(statement, parameter, RowBounds.DEFAULT, handler);
  }

  @Override
  public void select(String statement, ResultHandler handler) {
    select(statement, null, RowBounds.DEFAULT, handler);
  }

  @Override
  public void select(String statement, Object parameter, RowBounds rowBounds, ResultHandler handler) { // 有啥用 hama
    try {
      MappedStatement ms = configuration.getMappedStatement(statement); // 取出MappedStatement
      executor.query(ms, wrapCollection(parameter), rowBounds, handler); // 用executor执行查询，然后并没有返回
    } catch (Exception e) {
      throw ExceptionFactory.wrapException("Error querying database.  Cause: " + e, e);
    } finally {
      ErrorContext.instance().reset(); // hama
    }
  }

  @Override
  public int insert(String statement) {
    return insert(statement, null);
  }

  @Override
  public int insert(String statement, Object parameter) {
    return update(statement, parameter);
  }

  @Override
  public int update(String statement) {
    return update(statement, null);
  }

  @Override
  public int update(String statement, Object parameter) { // 更新
    try {
      dirty = true; // 标记dirty
      MappedStatement ms = configuration.getMappedStatement(statement); // 获得MappedStatement
      return executor.update(ms, wrapCollection(parameter)); // 用executor执行update
    } catch (Exception e) {
      throw ExceptionFactory.wrapException("Error updating database.  Cause: " + e, e);
    } finally {
      ErrorContext.instance().reset(); // hama
    }
  }

  @Override
  public int delete(String statement) { // 使用update
    return update(statement, null);
  }

  @Override
  public int delete(String statement, Object parameter) {
    return update(statement, parameter);
  }

  @Override
  public void commit() {
    commit(false);
  }

  @Override
  public void commit(boolean force) {
    try {
      executor.commit(isCommitOrRollbackRequired(force)); // 使用executor执行提交
      dirty = false; // 提交后没有脏数据
    } catch (Exception e) {
      throw ExceptionFactory.wrapException("Error committing transaction.  Cause: " + e, e);
    } finally {
      ErrorContext.instance().reset(); // hama
    }
  }

  @Override
  public void rollback() {
    rollback(false);
  }

  @Override
  public void rollback(boolean force) { // 回滚
    try {
      executor.rollback(isCommitOrRollbackRequired(force)); // 用executor执行回滚
      dirty = false; // 回滚后没有脏数据
    } catch (Exception e) {
      throw ExceptionFactory.wrapException("Error rolling back transaction.  Cause: " + e, e);
    } finally {
      ErrorContext.instance().reset(); // hama
    }
  }

  @Override
  public List<BatchResult> flushStatements() { // hama
    try {
      return executor.flushStatements();
    } catch (Exception e) {
      throw ExceptionFactory.wrapException("Error flushing statements.  Cause: " + e, e);
    } finally {
      ErrorContext.instance().reset();
    }
  }

  @Override
  public void close() { // 关闭session
    try {
      executor.close(isCommitOrRollbackRequired(false)); // 用executor执行close
      closeCursors(); // hama
      dirty = false; // close后没有脏数据
    } finally {
      ErrorContext.instance().reset();
    }
  }

  private void closeCursors() {
    if (cursorList != null && cursorList.size() != 0) { // 还有游标存在
      for (Cursor<?> cursor : cursorList) { // 遍历关闭所有游标
        try {
          cursor.close();
        } catch (IOException e) {
          throw ExceptionFactory.wrapException("Error closing cursor.  Cause: " + e, e);
        }
      }
      cursorList.clear(); // 清空游标list
    }
  }

  @Override
  public Configuration getConfiguration() { // 获得mybatis的Configuration
    return configuration;
  }

  @Override
  public <T> T getMapper(Class<T> type) { // 获得mapper，是啥 hama
    return configuration.<T>getMapper(type, this);
  }

  @Override
  public Connection getConnection() { // 从executor获得事务的连接
    try {
      return executor.getTransaction().getConnection();
    } catch (SQLException e) {
      throw ExceptionFactory.wrapException("Error getting a new connection.  Cause: " + e, e);
    }
  }

  @Override
  public void clearCache() { // 清空缓存
    executor.clearLocalCache(); // 但是究竟做了啥 hama
  }

  private <T> void registerCursor(Cursor<T> cursor) { // 注册游标
    if (cursorList == null) {
      cursorList = new ArrayList<Cursor<?>>();
    }
    cursorList.add(cursor); // 游标加入list
  }

  private boolean isCommitOrRollbackRequired(boolean force) {
    return (!autoCommit && dirty) || force; // 非自动提交且有脏数据 或 强制提交开关开启
  }

  private Object wrapCollection(final Object object) { // 包装，用于送入update方法的参数
    if (object instanceof Collection) { // 是个Collection
      StrictMap<Object> map = new StrictMap<Object>();
      map.put("collection", object); // <"collenction", object>
      if (object instanceof List) { // 是个Collection且是个List
        map.put("list", object); // <"list", object>
      }
      return map;
    } else if (object != null && object.getClass().isArray()) { // 是个非null的数组
      StrictMap<Object> map = new StrictMap<Object>();
      map.put("array", object); // <"array", object>
      return map;
    }
    return object; // 其他情况不包装，直接返回object
  }

  public static class StrictMap<V> extends HashMap<String, V> {

    private static final long serialVersionUID = -5741767162221585340L;

    @Override
    public V get(Object key) {
      if (!super.containsKey(key)) {
        throw new BindingException("Parameter '" + key + "' not found. Available parameters are " + this.keySet());
      }
      return super.get(key);
    }

  }

}
