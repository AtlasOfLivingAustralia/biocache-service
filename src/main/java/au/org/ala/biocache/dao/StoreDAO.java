package au.org.ala.biocache.dao;

import java.io.IOException;
import java.util.Optional;

/**
 * Permanent data store interface.
 */
public interface StoreDAO {
    <T> Optional<T> get(Class<T> dataClass, String key) throws IOException;

    <T> void put(String key, T data) throws IOException;

    <T> Boolean delete(Class<T> dataClass, String key) throws IOException;
}
