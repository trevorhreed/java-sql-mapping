package sqlmapping;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.exception.ExceptionUtils;
import maptree.MapTree;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

public class SqlSelector extends SqlExecutor {
  private MapTree results;
  private List<JoinMapping> joinMappings;

  /**
   * TREVOR: fill this in...
   * @param namedParameterJdbcTemplate
   */
  protected SqlSelector(NamedParameterJdbcTemplate namedParameterJdbcTemplate){
    super(namedParameterJdbcTemplate);
  }

  /**
   * Sets a named parameter for SQL statements.
   *
   * <p><b>Details:</b></p>
   * Sets a named parameter on the SqlSelector.
   *
   * @param key the key of the named parameter
   * @param val the value of the named parameter
   * @return the instance of SqlSelector (for chaining)
   */
  public SqlSelector param(String key, Object val) {
    if(params == null){
      params = new MapSqlParameterSource();
    }
    params.addValue(key, val);
    return this;
  }

  /**
   * ...
   * @param key
   * @return
   */
  public SqlSelector map(String key){
    return map(key, key, null);
  }

  /**
   * Creates a mapping between a SQL column and a location in a tree-like Map object.
   *
   * <p><b>Details:</b></p>
   * ...
   *
   * @param key the key of the SQL column whose value is to be mapped
   * @param path the location in a Map object where the value of the SQL column (denoted by {@code key} is to be put
   * @return the instance of SqlSelector (for chaining)
   */
  public SqlSelector map(String key, String path){
    return map(key, path, null);
  }

  /**
   *
   * @param key
   * @param transformer
   * @return
   */
  public SqlSelector map(String key, Transformer transformer){
    return map(key, key, transformer);
  }

  /**
   * Creates a mapping between a SQL column, a location in a tree-like Map object, and a transformation.
   *
   * <p><b>Details:</b></p>
   * ...
   *
   * @param key the key of the SQL column whose value is to be mapped
   * @param path the location in a Map object where the value of the SQL column (denoted by {@code key} is to be put
   * @param transformer an instance of a Transformer class, which transforms the data from a SQL query
   * @return the instance of SqlSelector (for chaining)
   */
  public SqlSelector map(String key, String path, Transformer transformer) {
    if(mappings == null){
      mappings = new ArrayList<>();
    }
    mappings.add(new Mapping(path, key, transformer));
    return this;
  }


  /**
   * Specifies how to perform a join (See {@link #joinToListByPath(String, String)}).
   *
   * <p>
   *   <b>Details:</b>
   *   When {@link #joinToListByPath(String, String)} is called, it will use the data passed to this function in order to join data
   * </p>
   * @param parentKey the key of the parent data to match against
   * @param childKey the key of the child data to match against
   * @param childPath the path (relative to the parent) in which to put the joined data
   * @return
   */
  public SqlSelector on(String parentKey, String childKey, String childPath){
    if(joinMappings == null){
      joinMappings = new ArrayList<>();
    }
    joinMappings.add(new JoinMapping(parentKey, childKey, childPath));
    return this;
  }

  /**
   * Joins data retrieved from the {@code sql} query with data already retrieved by this SqlSelector instance.
   *
   * <p>
   *   <b>Details:</b>
   *   Joins data from the {@code sql} query (the child data) to the underlying MapTree (the parent data) at the path specified by {@code parentPath}.  This function uses the mappings created by {@link #on(String, String, String)} to match records in the parent data with records in the child data.
   * </p>
   * @param parentPath the path at which to join the data
   * @param sql the SQL query to be executed, the results of which will be joined with the underlying MapTree
   * @return
   */
  public SqlSelector joinToMapByPath(String parentPath, String sql){
    List<MapTree> children = queryForList(sql);
    MapTree parent = (MapTree)results.getByPath(parentPath);
    joinChildren(parent, children);
    joinMappings = null;
    return this;
  }

  /**
   * Joins data retrieved from the {@code sql} query with data already retrieved by this SqlSelector instance.
   *
   * <p>
   *   <b>Details:</b>
   *   Joins data from the {@code sql} query (the child data) to the underlying MapTree (the parent data) at the path specified by {@code parentPath}.  This function uses the mappings created by {@link #on(String, String, String)} to match records in the parent data with records in the child data.
   * </p>
   * @param parentPath the path at which to join the data
   * @param sql the SQL query to be executed, the results of which will be joined with the underlying MapTree
   * @return
   */
  public SqlSelector joinToListByPath(String parentPath, String sql){
    List<MapTree> children = queryForList(sql);
    List<MapTree> parents = (List)results.getByPath(parentPath);
    for(MapTree parent : parents){
      joinChildren(parent, children);
    }
    joinMappings = null;
    return this;
  }

  private void joinChildren(MapTree parent, List<MapTree> children){
    for(JoinMapping mapping : joinMappings){
      Object parentValue = parent.get(mapping.parentKey);
      Object childList = parent.get(mapping.childPath);
      if(childList == null || !(childList instanceof List)){
        childList = new ArrayList<MapTree>();
        parent.put(mapping.childPath, childList);
      }
      for(MapTree child : children){
        Object childValue = child.get(mapping.childKey);
        if(childValue != null && childValue.equals(parentValue)){
          ((List)childList).add(child);
        }
      }
    }
  }

  /**
   *
   * @return
   */
  public MapTree getResults(){
    return results;
  }

  /**
   * TREVOR: fill this in
   * @param path
   * @param sql
   * @return
   */
  public SqlSelector putObjectQueryByPath(String path, String sql){
    if(results == null){
      results = new MapTree();
    }
    results.putByPathUnique(path, queryForObject(sql));
    return this;
  }

  public Object queryForObject(String sql){
    return queryForObject(sql, Object.class);
  }

  /**
   * Queries for and returns a single object
   *
   * <p><b>Details:</b></p>
   * ...
   *
   * @param sql the SQL query to be executed
   * @return the result of the SQL query
   */
  public <T> T queryForObject(String sql, Class<T> type){
    if(params == null){
      params = new MapSqlParameterSource();
    }
    T result = namedParameterJdbcTemplate.queryForObject(
      sql,
      params,
      type
    );
    mappings = new ArrayList<>();
    return result;
  }

  /**
   * TREVOR: fill this in
   * @param path
   * @param sql
   * @return
   */
  public SqlSelector putMapQueryByPath(String path, String sql){
    if(results == null){
      results = new MapTree();
    }
    results.putByPathUnique(path, queryForMap(sql));
    return this;
  }

  /**
   * Queries for and returns a Map of objects
   *
   * <p><b>Details:</b></p>
   * ...
   *
   * @param sql the SQL query to be executed
   * @return the instance of SqlSelector (for chaining)
   */
  public MapTree queryForMap(String sql, boolean ignoreEmptyResult){
    if(params == null){
      params = new MapSqlParameterSource();
    }
    try {
      Map<String, Object> row = namedParameterJdbcTemplate.queryForMap(
        sql,
        params
      );
      MapTree result = getMapFromRow(row);
      mappings = new ArrayList<>();
      return result;
    }catch(EmptyResultDataAccessException e){
      if(!ignoreEmptyResult){
        logger.warning("EmptyResultDataAccessException occurred in SqlSelector::queryForMap");
        logger.warning(ExceptionUtils.getStackTrace(e));
      }
      return new MapTree();
    }
  }
  public MapTree queryForMap(String sql){
    return queryForMap(sql, true);
  }

  /**
   * TREVOR: fill this in...
   * @param path
   * @param sql
   * @return
   */
  public SqlSelector putListQueryByPath(String path, String sql){
    if(results == null){
      results = new MapTree();
    }
    results.putByPathUnique(path, queryForList(sql));
    return this;
  }

  /**
   * Queries for and returns a List of Maps of objects
   *
   * <p><b>Details:</b></p>
   * ...
   *
   * @param sql the SQL query to be executed
   * @return the instance of SqlSelector (for chaining)
   */
  public List<MapTree> queryForList(String sql){
    if(params == null){
      params = new MapSqlParameterSource();
    }
    List<MapTree> list = new ArrayList<>();
    List<Map<String, Object>> rows = namedParameterJdbcTemplate.queryForList(
      sql,
      params
    );
    for(Map<String, Object> row : rows){
      list.add(getMapFromRow(row));
    }
    mappings = new ArrayList<>();
    return list;
  }
  private MapTree getMapFromRow(Map row){
    MapTree map = new MapTree();
    if(mappings != null){
      for(Mapping mapping : mappings){
        Object value = row.get(mapping.key);
        if(mapping.transformer != null){
          value = mapping.transformer.transform(value, row);
        }
        map.putByPathUnique(mapping.path, value);
      }
    }
    return map;
  }

  private class JoinMapping {
    String parentKey;
    String childKey;
    String childPath;
    public JoinMapping(String parentKey, String childKey, String childPath){
      this.parentKey = parentKey;
      this.childKey = childKey;
      this.childPath = childPath;
    }

    public String toString(){
      return "["+parentKey+", "+childKey+", "+childPath+"]";
    }
  }
}
