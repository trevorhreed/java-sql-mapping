package sqlmapping;

import javax.inject.Inject;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

@Service("SqlExecuterFactory")
public class SqlExecutorFactory {
  @Inject
  private NamedParameterJdbcTemplate namedParameterJdbcTemplate;
  public SqlSelector createSqlSelector(){
    return new SqlSelector(namedParameterJdbcTemplate);
  }
  public SqlUpdater createSqlUpdater(){
    return new SqlUpdater(namedParameterJdbcTemplate);
  }
}
