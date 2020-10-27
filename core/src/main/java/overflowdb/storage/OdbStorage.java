package overflowdb.storage;

import overflowdb.Node;
import overflowdb.NodeDb;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class OdbStorage implements AutoCloseable {
  /** increase this number when persistence format changes (usually driven by changes in the NodeSerializer)
   * this protects us from attempting to open outdated formats */
  public static final int STORAGE_FORMAT_VERSION = 2;

  public static final String METADATA_KEY_STORAGE_FORMAT_VERSION = "STORAGE_FORMAT_VERSION";
  public static final String METADATA_KEY_STRING_TO_INT_MAX_ID = "STRING_TO_INT_MAX_ID";
  private static final String INDEX_PREFIX = "index_";

  private final Logger logger = LoggerFactory.getLogger(getClass());
  protected final NodeSerializer nodeSerializer;
  protected final Optional<NodeDeserializer> nodeDeserializer;

  private final File mvstoreFile;
  private final boolean doPersist;
  protected MVStore mvstore;
  private MVMap<Long, byte[]> nodesMVMap;
  private MVMap<String, String> metadataMVMap;
  private MVMap<String, Integer> stringToIntMappings;
  private boolean closed;
  private final AtomicInteger stringToIntMappingsMaxId = new AtomicInteger(0);
  private ArrayList<String> stringToIntReverseMappings;

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
      if (mvstoreFile.exists() && mvstoreFile.length() > 0) {
        verifyStorageVersion();
        initializeStringToIntMaxId();
      }
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

  private void initializeStringToIntMaxId() {
    MVMap<String, String> metadata = getMetaDataMVMap();
    if (metadata.containsKey(METADATA_KEY_STRING_TO_INT_MAX_ID)) {
      int maxIndexFromStorage = Integer.parseInt(metadata.get(METADATA_KEY_STRING_TO_INT_MAX_ID));
      stringToIntMappingsMaxId.set(maxIndexFromStorage);
    }
  }

  /** storage version must be exactly the same */
  private void verifyStorageVersion() {
    ensureMVStoreAvailable();
    MVMap<String, String> metaData = getMetaDataMVMap();
    if (!metaData.containsKey(METADATA_KEY_STORAGE_FORMAT_VERSION)) {
      throw new BackwardsCompatibilityError("storage metadata does not contain version number, this must be an old format.");
    }

    String storageFormatVersionString = metaData.get(METADATA_KEY_STORAGE_FORMAT_VERSION);
    int storageFormatVersion = Integer.parseInt(storageFormatVersionString);
    if (storageFormatVersion != STORAGE_FORMAT_VERSION) {
      throw new BackwardsCompatibilityError(String.format(
          "attempting to open storage with different version: %s; this version of overflowdb requires the version to be exactly %s",
          storageFormatVersion, STORAGE_FORMAT_VERSION));
    }
  }

  public void persist(final NodeDb node) {
    final long id = node.ref.id();
    persist(id, serialize(node));
  }

  public byte[] serialize(NodeDb node) {
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

  public <A extends Node> A readNode(final long id) throws IOException {
    return (A) nodeDeserializer.get().deserialize(getNodesMVMap().get(id));
  }

  /** flush any remaining changes in underlying storage to disk */
  public void flush() {
    if (mvstore != null) {
      logger.trace("flushing to disk");
      getMetaDataMVMap().put(METADATA_KEY_STORAGE_FORMAT_VERSION, String.format("%s", STORAGE_FORMAT_VERSION));
      getMetaDataMVMap().put(METADATA_KEY_STRING_TO_INT_MAX_ID, String.format("%s", stringToIntMappingsMaxId.get()));
      mvstore.commit();
    }
  }

  @Override
  public void close() {
    closed = true;
    logger.debug("closing " + getClass().getSimpleName());
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

  public synchronized MVMap<Long, byte[]> getNodesMVMap() {
    ensureMVStoreAvailable();
    if (nodesMVMap == null)
      nodesMVMap = mvstore.openMap("nodes");
    return nodesMVMap;
  }

  public synchronized MVMap<String, String> getMetaDataMVMap() {
    ensureMVStoreAvailable();
    if (metadataMVMap == null)
      metadataMVMap = mvstore.openMap("metadata");
    return metadataMVMap;
  }

  public synchronized MVMap<String, Integer> getStringToIntMappings() {
    ensureMVStoreAvailable();
    if (stringToIntMappings == null)
      stringToIntMappings = mvstore.openMap("stringToIntMappings");

    if (stringToIntReverseMappings == null) {
      int mappingsCount = stringToIntMappings.size();
      stringToIntReverseMappings = new ArrayList<>(mappingsCount);
      // initialize list with correct size - we want to use it as an reverse index, and ArrayList.ensureCapacity doesn't actually grow the list...
      for (int i = 0; i <= mappingsCount; i++) {
        stringToIntReverseMappings.add(null);
      }
      stringToIntMappings.forEach((string, id) -> stringToIntReverseMappings.set(id, string));
    }

    return stringToIntMappings;
  }

  public int lookupOrCreateStringToIntMapping(String s) {
    final MVMap<String, Integer> mappings = getStringToIntMappings();
    if (mappings.containsKey(s)) {
      return mappings.get(s);
    } else {
      return createStringToIntMapping(s);
    }
  }

  private int createStringToIntMapping(String s) {
    int index = stringToIntMappingsMaxId.incrementAndGet();
    getStringToIntMappings().put(s, index);

    stringToIntReverseMappings.add(null); // ensure there's enough space
    stringToIntReverseMappings.set(index, s);
    return index;
  }

  public String reverseLookupStringToIntMapping(int stringId) {
    getStringToIntMappings(); //ensure everything is initialized
    return stringToIntReverseMappings.get(stringId);
  }

  private void ensureMVStoreAvailable() {
    if (mvstore == null) {
      mvstore = initializeMVStore();
    }
  }

  private MVStore initializeMVStore() {
    final MVStore store = new MVStore.Builder()
        .fileName(mvstoreFile.getAbsolutePath())
        .autoCommitBufferSize(1024 * 8)
        .compress()
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
