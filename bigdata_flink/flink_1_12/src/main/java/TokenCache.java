import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheLoader;

import java.util.Map;

/**
 * Author: jiwei01
 * Date: 2022/7/5 19:25
 * Package: PACKAGE_NAME
 * Description:
 */
public class TokenCache extends MemoryCache<String, Map<String, Object>>{
    @Override
    protected CacheBuilder<Object, Object> getCacheBuilder(CacheBuilder<Object, Object> cacheBuilder) {
        return null;
    }

    @Override
    protected CacheLoader<String, Map<String, Object>> getCacheLoader() {
        return null;
    }
}