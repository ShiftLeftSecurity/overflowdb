package overflowdb.storage;

import overflowdb.OdbNode;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class OdbStorage implements AutoCloseable {
  private static final String INDEX_PREFIX = "index_";
  private final Logger logger = LoggerFactory.getLogger(getClass());
  protected final NodeSerializer nodeSerializer;
  protected final Optional<NodeDeserializer> nodeDeserializer;

  private final File mvstoreFile;
  private final boolean doPersist;
  private MVStore mvstore; // initialized in `getNodesMVMap`
  private MVMap<Long, byte[]> nodesMVMap;
  private boolean closed;

  public static OdbStorage createWithTempFile(
      final NodeDeserializer nodeDeserializer, final boolean enableSerializationStats) {
    return new OdbStorage(Optional.empty(), Optional.ofNullable(nodeDeserializer), enableSerializationStats);
  }

  /**
   * create with specific mvstore file - which may or may not yet exist.
   * mvstoreFile won't be deleted at the end (unlike temp file constructors above)
   */
  public static OdbStorage createWithSpecificLocation(
      final NodeDeserializer nodeDeserializer, final File mvstoreFile, final boolean enableSerializationStats) {
    return new OdbStorage(Optional.ofNullable(mvstoreFile), Optional.ofNullable(nodeDeserializer), enableSerializationStats);
  }

  /**
   * create with specific mvstore file - which may or may not yet exist.
   * mvstoreFile won't be deleted at the end (unlike temp file constructors above)
   */
  public static OdbStorage createWithSpecificLocation(final File mvstoreFile, final boolean enableSerializationStats) {
    return new OdbStorage(Optional.ofNullable(mvstoreFile), Optional.empty(), enableSerializationStats);
  }

  private OdbStorage(
      final Optional<File> mvstoreFileMaybe,
      final Optional<NodeDeserializer> nodeDeserializer,
      final boolean enableSerializationStats) {
    this.nodeSerializer = new NodeSerializer(enableSerializationStats);
    this.nodeDeserializer = nodeDeserializer;

    if (mvstoreFileMaybe.isPresent()) {
      this.doPersist = true;
      mvstoreFile = mvstoreFileMaybe.get();
    } else {
      try {
        this.doPersist = false;
        mvstoreFile = File.createTempFile("mvstore", ".bin");
        mvstoreFile.deleteOnExit(); // `.close` will also delete it, this is just in case users forget to call it
      } catch (IOException e) {
        throw new RuntimeException("cannot create tmp file for mvstore", e);
      }
    }
    logger.trace("storage file: " + mvstoreFile);
  }

  public void persist(final OdbNode node) {
    final long id = node.ref.id;
    persist(id, serialize(node));
  }

  public byte[] serialize(OdbNode node) {
    try {
      return nodeSerializer.serialize(node);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void persist(long id, byte[] node) {
    if (!closed) {
      getNodesMVMap().put(id, node);
    }
  }

  public <A extends Vertex> A readNode(final long id) throws IOException {
    return (A) nodeDeserializer.get().deserialize(getNodesMVMap().get(id));
  }

  /** flush any remaining changes in underlying storage to disk */
  public void flush() {
    if (mvstore != null) {
      logger.debug("flushing to disk");
      mvstore.commit();
    }
  }

  @Override
  public void close() {
    closed = true;
    logger.info("closing " + getClass().getSimpleName());
    flush();
    if (mvstore != null) mvstore.close();
    if (!doPersist) mvstoreFile.delete();
  }

  public File getStorageFile() {
    return mvstoreFile;
  }

  public void removeNode(final Long id) {
    getNodesMVMap().remove(id);
  }

  public Set<Map.Entry<Long, byte[]>> allNodes() {
    return getNodesMVMap().entrySet();
  }

  public NodeSerializer getNodeSerializer() {
    return nodeSerializer;
  }

  public MVMap<Long, byte[]> getNodesMVMap() {
    if (mvstore == null) {
      mvstore = initializeMVStore();
    }
    if (nodesMVMap == null)
      nodesMVMap = mvstore.openMap("nodes");
    return nodesMVMap;
  }

  private MVStore initializeMVStore() {
    final MVStore store = new MVStore.Builder()
        .fileName(mvstoreFile.getAbsolutePath())
        .autoCommitBufferSize(1024 * 8)
        .open();
    return store;
  }

  public Optional<NodeDeserializer> getNodeDeserializer() {
    return nodeDeserializer;
  }

  private Map<String, String> getIndexNameMap(MVStore store) {
    return store
        .getMapNames()
        .stream()
        .filter(s -> s.startsWith(INDEX_PREFIX))
        .collect(Collectors.toConcurrentMap(s -> removeIndexPrefix(s), s -> s));
  }

  public Set<String> getIndexNames() {
    return getIndexNameMap(mvstore).keySet();
  }

  private String removeIndexPrefix(String s) {
    assert s.startsWith(INDEX_PREFIX);
    return s.substring(INDEX_PREFIX.length());
  }

  public MVMap<Object, long[]> openIndex(String indexName) {
    final String mapName = getIndexMapName(indexName);
    return mvstore.openMap(mapName);
  }

  private String getIndexMapName(String indexName) {
    return INDEX_PREFIX + indexName;
  }

  public void clearIndices() {
    getIndexNames().forEach(this::clearIndex);
  }

  public void clearIndex(String indexName) {
    openIndex(indexName).clear();
  }
}
