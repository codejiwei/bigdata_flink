import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava18.com.google.common.cache.LoadingCache;
import org.apache.http.client.utils.CloneUtils;

import java.util.concurrent.ExecutionException;

/**
 * Author: jiwei01
 * Date: 2022/7/5 18:58
 * Package: PACKAGE_NAME
 * Description:
 */
public abstract class MemoryCache<PK, T>{
    private LoadingCache<PK, T> cache;
    protected abstract CacheBuilder<Object, Object> getCacheBuilder(CacheBuilder<Object, Object> cacheBuilder);
    protected abstract CacheLoader<PK, T> getCacheLoader();
    protected LoadingCache<PK, T> getCache() {
        return cache;
    }

    public T getValue(PK pk) throws Exception {
        try {
            return CloneUtils.cloneObject(this.cache.get(pk));
        } catch (CloneNotSupportedException | ExecutionException e) {
            throw new Exception(e);
        }
    }

    public void setValue(PK pk, T t) {
        this.cache.put(pk, t);
    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        CacheLoader<PK, T> cacheLoader = this.getCacheLoader();
//        CacheBuilder<Object, Object> cacheBuilder = this.getCacheBuilder(CacheBuilder.newBuilder());
//        this.cache = cacheBuilder.build(cacheLoader);
//    }
}