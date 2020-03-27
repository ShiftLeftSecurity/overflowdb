package io.shiftleft.overflowdb.storage;

import io.shiftleft.overflowdb.OdbNode;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.mapdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class OdbStorage implements AutoCloseable {
  private static final String INDEX_PREFIX = "odb_index_";
  private static final long COMMIT_BATCH_SIZE = 1024 * 16;
  private final Logger logger = LoggerFactory.getLogger(getClass());
  protected final NodeSerializer nodeSerializer;
  protected final Optional<NodeDeserializer> nodeDeserializer;

  private final File mvstoreFile;
  private final boolean doPersist;
//  private MVStore mvstore; // initialized in `getNodesMVMap`
//  private MVMap<Long, byte[]> nodesMVMap;
  private DB db;
  private boolean closed;
  private BTreeMap<Long, byte[]> nodesTreeMap;

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
      this.doPersist = false;
//      mvstoreFile = null;
      try {
        mvstoreFile = File.createTempFile("mvstore", ".bin");
        mvstoreFile.delete();
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

  public static long commitCounter = 0L;

  public void persist(long id, byte[] node) {
    if (!closed) {
//      logger.info("Persist " + id + " with map size = " + getNodesMVMap().size());
      getNodesMVMap().put(id, node);
      ++commitCounter;
//      if (commitCounter > COMMIT_BATCH_SIZE) {
//        db.commit();
//        commitCounter = 0L;
//      }
    }
  }

  public <A extends Vertex> A readNode(final long id) throws IOException {
    return (A) nodeDeserializer.get().deserialize(getNodesMVMap().get(id));
  }

  @Override
  public void close() {
    closed = true;
    logger.info("closing " + getClass().getSimpleName() + " file=" + mvstoreFile);
    if (db != null) {
      db.commit();
      if (nodesTreeMap != null) {
//        logger.info("Closing node map: " + nodesTreeMap.size());
//        nodesTreeMap.close();
      }
//      db.getStore().compact();
      db.close();

      assert db.isClosed();
    }
    if (!doPersist) {
      logger.info("Deleting file: " + mvstoreFile);
      mvstoreFile.delete();
    }
  }

  public File getStorageFile() {
    return mvstoreFile;
  }

  public void removeNode(final Long id) {
    getNodesMVMap().remove(id);
  }

  public Set<Map.Entry<Long, byte[]>> allNodes() {
    return getNodesMVMap().getEntries();
  }

  public NodeSerializer getNodeSerializer() {
    return nodeSerializer;
  }

  private BTreeMap<Long, byte[]> getNodesMVMap() {
    if (db == null) {
      db = initializeStore();

    }
    if (nodesTreeMap == null) {
//      db.getStore().put()
      nodesTreeMap = db.treeMap("nodes")
          .keySerializer(Serializer.LONG)
          .valueSerializer(Serializer.BYTE_ARRAY)
          .createOrOpen();
      logger.info("Opened node map with " + nodesTreeMap.size() + " entries");
    }
    return nodesTreeMap;
  }

  private DB initializeStore() {
    logger.info("initializeStore: " + mvstoreFile);
    closed = false;
    if (mvstoreFile != null) {
      if (mvstoreFile.exists() && mvstoreFile.length() == 0) {
        logger.info("Removing empty file.");
        mvstoreFile.delete();
      }
      final DB db = DBMaker.fileDB(mvstoreFile.getAbsoluteFile())
//          .concurrencyScale(8)
//          .fileLockDisable()
//          .fileChannelEnable()
//          .cleanerHackEnable()
          .fileLockWait()
          .fileMmapPreclearDisable()
//          .transactionEnable()
          .fileMmapEnable()
//          .concurrencyDisable()
//          .executorEnable()
          .make();
      db.getAllNames().forEach(System.out::println);
      return db;
    }
    else {
      logger.info("MapDB tempFileDB");
      return DBMaker.tempFileDB()
//          .transactionEnable()
          .make();
    }
  }

  public Optional<NodeDeserializer> getNodeDeserializer() {
    return nodeDeserializer;
  }

  private Map<String, String> getIndexNameMap(DB db) {
    ArrayList<String> names = new ArrayList<>();
    db.getAllNames().forEach(names::add);
    return names
        .stream()
        .filter(s -> s.startsWith(INDEX_PREFIX))
        .collect(Collectors.toConcurrentMap(this::removeIndexPrefix, s -> s));
  }

  public Set<String> getIndexNames() {
    return getIndexNameMap(db).keySet();
  }

  private String removeIndexPrefix(String s) {
    assert s.startsWith(INDEX_PREFIX);
    return s.substring(INDEX_PREFIX.length());
  }

  public BTreeMap<Object, long[]> openIndex(String indexName) {
    final String mapName = getIndexMapName(indexName);
    // TODO: Replace JAVA serializer with a concrete value type for an index
    return db.treeMap(mapName).keySerializer(Serializer.JAVA).valueSerializer(Serializer.LONG_ARRAY).createOrOpen();
  }

  private String getIndexMapName(String indexName) {
    return INDEX_PREFIX + indexName;
  }

  public void clearIndices() {
    getIndexNames().forEach(this::clearIndex);
  }

  public void clearIndex(String indexName) {
    logger.info("Clearing index: " + indexName);
    openIndex(indexName).clear();
    db.commit();
  }
}
