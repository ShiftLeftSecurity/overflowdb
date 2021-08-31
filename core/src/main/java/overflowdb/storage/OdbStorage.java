package overflowdb.storage;

import org.h2.mvstore.FileStore;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import overflowdb.util.StringInterner;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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
  public static final String METADATA_KEY_LIBRARY_VERSIONS_MAX_ID = "LIBRARY_VERSIONS_MAX_ID";
  public static final String METADATA_PREFIX_LIBRARY_VERSIONS = "LIBRARY_VERSIONS_ENTRY_";
  private static final String INDEX_PREFIX = "index_";

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final File mvstoreFile;
  private FileStore mvstoreFileStore;
  protected MVStore mvstore;
  private MVMap<Long, byte[]> nodesMVMap;
  private MVMap<String, String> metadataMVMap;
  private MVMap<String, Integer> stringToIntMappings;
  private boolean closed;
  private final AtomicInteger stringToIntMappingsMaxId = new AtomicInteger(0);
  private ArrayList<String> stringToIntReverseMappings;
  private int libraryVersionsIdCurrentRun;

  public static OdbStorage createWithTempFile() {
    return new OdbStorage(Optional.empty());
  }

  /**
   * create with specific mvstore file - which may or may not yet exist.
   * mvstoreFile won't be deleted at the end (unlike temp file constructors above)
   */
  public static OdbStorage createWithSpecificLocation(final File mvstoreFile) {
    return new OdbStorage(Optional.ofNullable(mvstoreFile));
  }

  private OdbStorage(final Optional<File> mvstoreFileMaybe) {
    if (mvstoreFileMaybe.isPresent()) {
      mvstoreFile = mvstoreFileMaybe.get();
      if (mvstoreFile.exists() && mvstoreFile.length() > 0) {
        verifyStorageVersion();
        initializeStringToIntMaxId();
      }
    } else {
      try {
        mvstoreFile = File.createTempFile("mvstore", ".bin");
        if (!System.getProperty("os.name").toLowerCase().contains("win")) {
          mvstoreFileStore = new FileStore();
          boolean readOnly = false;
          char[] encryptionKey = null;
          mvstoreFileStore.open(mvstoreFile.getAbsolutePath(), readOnly, encryptionKey);
          /** Note: we're deleting the temporary storage file as early as possible on *nix systems, i.e. while it's still running
            * This is so we don't fill up `/tmp` if the JVM gets killed.
            **/
          mvstoreFile.delete();
        } else {
          mvstoreFile.deleteOnExit();
        }
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

  public void persist(long id, byte[] node) {
    if (!closed) {
      getNodesMVMap().put(id, node);
    }
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
      stringToIntMappings.forEach((string, id) -> {
        ensureCapacity(stringToIntReverseMappings, id + 1);
        stringToIntReverseMappings.set(id, string);
      });
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
    final int index = stringToIntMappingsMaxId.incrementAndGet();
    getStringToIntMappings().put(s, index);

    ensureCapacity(stringToIntReverseMappings, index + 1);
    stringToIntReverseMappings.set(index, s);
    return index;
  }

  /**
   * initialize list with correct size - we want to use it as an reverse index,
   * and ArrayList.ensureCapacity doesn't actually grow the list... */
  private void ensureCapacity(ArrayList array, int requiredMinSize) {
    while (array.size() < requiredMinSize) {
      array.add(null);
    }
  }

  public String reverseLookupStringToIntMapping(int stringId) {
    getStringToIntMappings(); //ensure everything is initialized
    String string = stringToIntReverseMappings.get(stringId);
    return StringInterner.intern(string);
  }

  private void ensureMVStoreAvailable() {
    if (mvstore == null) {
      mvstore = initializeMVStore();
      persistOdbLibraryVersion();
      this.libraryVersionsIdCurrentRun = initializeLibraryVersionsIdCurrentRun();
    }
  }

  private MVStore initializeMVStore() {
    MVStore.Builder builder = new MVStore.Builder()
        .autoCommitBufferSize(1024 * 8)
        .compress()
        .autoCommitDisabled();

    if (mvstoreFileStore != null) {
      builder.fileStore(mvstoreFileStore);
    } else {
      builder.fileName(mvstoreFile.getAbsolutePath());
    }

    return builder.open();
  }

  private int initializeLibraryVersionsIdCurrentRun() {
    MVMap<String, String> metaData = getMetaDataMVMap();
    final int res;
    if (metaData.containsKey(METADATA_KEY_LIBRARY_VERSIONS_MAX_ID)) {
      res = Integer.parseInt(metaData.get(METADATA_KEY_LIBRARY_VERSIONS_MAX_ID)) + 1;
    } else {
      res = 0;
    }

    metaData.put(METADATA_KEY_LIBRARY_VERSIONS_MAX_ID, "" + res);
    return res;
  }

  private Map<String, String> getIndexNameMap(MVStore store) {
    return store
        .getMapNames()
        .stream()
        .filter(s -> s.startsWith(INDEX_PREFIX))
        .collect(Collectors.toConcurrentMap(this::removeIndexPrefix, s -> s));
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

  public byte[] getSerializedNode(long nodeId) {
    return getNodesMVMap().get(nodeId);
  }

  private void persistOdbLibraryVersion() {
    Class clazz = getClass();
    String version = clazz.getPackage().getImplementationVersion();
    if (version != null) persistLibraryVersion(clazz.getCanonicalName(), version);
  }

  public void persistLibraryVersion(String name, String version) {
    String key = String.format("%s%d_%s", METADATA_PREFIX_LIBRARY_VERSIONS, libraryVersionsIdCurrentRun, name);
    getMetaDataMVMap().put(key, version);
  }

  public ArrayList<Map<String, String>> getAllLibraryVersions() {
    Map<Integer, Map<String, String>> libraryVersionsByRunId = new HashMap<>();
    getMetaDataMVMap().forEach((key, version) -> {
      if (key.startsWith(METADATA_PREFIX_LIBRARY_VERSIONS)) {
        String withoutPrefix = key.substring(METADATA_PREFIX_LIBRARY_VERSIONS.length());
        int firstDividerIndex = withoutPrefix.indexOf('_');
        int runId = Integer.parseInt(withoutPrefix.substring(0, firstDividerIndex));
        String library = withoutPrefix.substring(firstDividerIndex + 1);
        Map<String, String> versionInfos = libraryVersionsByRunId.computeIfAbsent(runId, i -> new HashMap<>());
        versionInfos.put(library, version);
      }
    });

    return new ArrayList<>(libraryVersionsByRunId.values());
  }
}
