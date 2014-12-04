package sqlmapping;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import maptree.MapTree;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;

public class SqlUpdater extends SqlExecutor {
  List<String> keyNames;
  GeneratedKeyHolder keyHolder;
  protected SqlUpdater(NamedParameterJdbcTemplate namedParameterJdbcTemplate){
    super(namedParameterJdbcTemplate);
  }

  /**
   * Specify an auto-generating key to be retreived after an INSERT.
   *
   * <p>
   *   <b>Details:</b>
   *   Specify an auto-generating key to be retreived after an INSERT.
   * </p>
   *
   * @param key the name of the key to be retrieved
   * @return the instance of SqlUpdater (for chaining)
   */
  public SqlUpdater key(String key){
    if(keyNames == null){
      keyNames = new ArrayList<>();
    }
    keyNames.add(key);
    return this;
  }

  /**
   * Retrieve the auto-generated key specfied with {@link #key(String)} as a Long.
   *
   * <p>
   *   <b>Details:</b>
   *   Retrieve the auto-generated key specfied with {@link #key(String)} as a Long. Calling this only makes sense after INSERTing into a table with an auto-generated key.
   * </p>
   *
   * @return the auto-generated key to be retrieved
   */
  public long getLongKey(){
    return keyHolder.getKey().longValue();
  }

  /**
   * Retrieve the auto-generated key specfied with {@link #key(String)} as an Int.
   *
   * <p>
   *   <b>Details:</b>
   *   Retrieve the auto-generated key specfied with {@link #key(String)} as a Int. Calling this only makes sense after INSERTing into a table with an auto-generated key.
   * </p>
   *
   * @return the auto-generated key to be retrieved
   */
  public int getIntKey(){
    return keyHolder.getKey().intValue();
  }

  /**
   * Retrieve the KeyHolder containing the auto-generated key specfied with {@link #key(String)}.
   *
   * <p>
   *   <b>Details:</b>
   *   Retrieve the KeyHolder containing the auto-generated key specfied with {@link #key(String)}. Calling this only makes sense after INSERTing into a table with an auto-generated key.
   * </p>
   *
   * @return a KeyHolder object contaiing the auto-generated key
   */
  public GeneratedKeyHolder getKeyHolder(){
    return keyHolder;
  }

  /**
   * Sets a named parameter for SQL statements.
   *
   * <p><b>Details:</b></p>
   * Sets a named parameter on the SqlSelector.
   *
   * @param key the key of the named parameter
   * @param val the value of the named parameter
   * @return the instance of SqlUpdater (for chaining)
   */
  public SqlUpdater param(String key, Object val){
    return param(key, val, null);
  }

  /**
   * Sets a named parameter for SQL statements.
   *
   * <p><b>Details:</b></p>
   * Sets a named parameter on the SqlSelector.
   *
   * @param key the key of the named parameter
   * @param val the value of the named parameter
   * @param transformer a transformation to be performed on the data before adding it as a parameter to the SQL statement
   * @return the instance of SqlUpdater (for chaining)
   */
  public SqlUpdater param(String key, Object val, Transformer transformer){
    if(params == null){
      params = new MapSqlParameterSource();
    }
    if(transformer != null){
      val = transformer.transform(val, new HashMap());
    }
    params.addValue(key, val);
    return this;
  }

  /**
   *
   * @param key
   * @return the instance of SqlUpdater (for chaining)
   */
  public SqlUpdater map(String key){
    return map(key, key, null);
  }

  /**
   *
   * @param path
   * @param key
   * @return the instance of SqlUpdater (for chaining)
   */
  public SqlUpdater map(String path, String key){
    return map(path, key, null);
  }

  /**
   *
   * @param key
   * @param transformer
   * @return the instance of SqlUpdater (for chaining)
   */
  public SqlUpdater map(String key, Transformer transformer){
    return map(key, key, transformer);
  }

  /**
   *
   * @param path
   * @param key
   * @param transformer
   * @return the instance of SqlUpdater (for chaining)
   */
  public SqlUpdater map(String path, String key, Transformer transformer){
    if(mappings == null){
      mappings = new ArrayList<>();
    }
    mappings.add(new Mapping(path, key, transformer));
    return this;
  }

  /**
   *
   * @param sql
   * @param params
   * @return the number of rows affected by the SQL statement
   */
  private int update(String sql, MapSqlParameterSource params){
    if(keyNames != null){
      keyHolder = new GeneratedKeyHolder();
      int count = namedParameterJdbcTemplate.update(
        sql,
        params,
        keyHolder,
        keyNames.toArray(new String[]{})
      );
      keyNames = null;
      return count;
    }else{
      int count = namedParameterJdbcTemplate.update(
        sql,
        params
      );
      return count;
    }
  }

  /**
   *
   * @param sql
   * @return
   */
  public int update(String sql){
    return update(sql, params);
  }

  /**
   *
   * @param sql
   * @param expectedRowCount
   * @return True if successful, otherwise False
   */
  public boolean update(String sql, int expectedRowCount){
    int count = update(sql);
    return count == expectedRowCount;
  }

  /**
   *
   * @param updateSql
   * @param insertSql
   * @return True if successful, otherwise False
   */
  public boolean upsert(String updateSql, String insertSql){
    int count = update(updateSql);
    if(count != 1){
      count = update(insertSql);
    }
    return count == 1;
  }

  /**
   *
   * @param sql
   * @param dataList
   * @return True if successful, otherwise False
   */
  public boolean updateList(String sql, List<Map> dataList){
    boolean success = true;
    for(Map data : dataList){
      if(!(data instanceof MapTree)){
        data = new MapTree(data);
      }
      MapSqlParameterSource vParams = new MapSqlParameterSource();
      for(Mapping mapping : mappings){
        Object value = data.get(mapping.path);
        if(mapping.transformer != null){
          value = mapping.transformer.transform(value, data);
        }
        vParams.addValue(mapping.key, value);
      }
      if(params != null){
        Iterator<Map.Entry<String, Object>> globalParams = params.getValues().entrySet().iterator();
        while(globalParams.hasNext()){
          Map.Entry<String, Object> entry = globalParams.next();
          vParams.addValue(entry.getKey(), entry.getValue());
        }
      }
      int count = update(sql, vParams);
      success = success && count == 1;
    }
    return success;
  }
}
