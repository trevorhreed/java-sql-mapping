package sqlmapping;

import java.util.List;
import java.util.logging.Logger;
import javax.inject.Inject;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

public abstract class SqlExecutor {
  protected static final Logger logger = Logger.getLogger("mcore-debug");
  @Inject protected NamedParameterJdbcTemplate namedParameterJdbcTemplate;
  protected MapSqlParameterSource params;
  protected List<Mapping> mappings;
  protected SqlExecutor(NamedParameterJdbcTemplate namedParameterJdbcTemplate){
    this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
  }
  protected class Mapping {
    String path;
    String key;
    Transformer transformer;
    public Mapping(String path, String key, Transformer transformer){
      this.path = path;
      this.key = key;
      this.transformer = transformer;
    }
  }
}
