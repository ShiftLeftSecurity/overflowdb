package io.shiftleft.overflowdb.storage;

import io.shiftleft.overflowdb.NodeRef;
import io.shiftleft.overflowdb.OdbNode;
import org.apache.commons.lang.math.IntRange;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class OdbStorage implements AutoCloseable {
  private static final String INDEX_PREFIX = "index_";
  public static final int SHARDS = 4;
  private final Logger logger = LoggerFactory.getLogger(getClass());
  protected final NodeSerializer nodeSerializer = new NodeSerializer();
  protected final Optional<NodeDeserializer> nodeDeserializer;

  private final File mvstoreFile;
  private MVStore mvstore; // initialized in `getNodesMVMap`
  private ArrayList<MVMap<Long, byte[]>> nodesMVMapList = new ArrayList(SHARDS);
  private boolean closed;

  public static OdbStorage createWithTempFile(final NodeDeserializer nodeDeserializer) {
    return new OdbStorage(Optional.empty(), Optional.ofNullable(nodeDeserializer));
  }

  /**
   * create with specific mvstore file - which may or may not yet exist.
   * mvstoreFile won't be deleted at the end (unlike temp file constructors above)
   */
  public static OdbStorage createWithSpecificLocation(
      final NodeDeserializer nodeDeserializer, final File mvstoreFile) {
    return new OdbStorage(Optional.ofNullable(mvstoreFile), Optional.ofNullable(nodeDeserializer));
  }

  /**
   * create with specific mvstore file - which may or may not yet exist.
   * mvstoreFile won't be deleted at the end (unlike temp file constructors above)
   */
  public static OdbStorage createWithSpecificLocation(final File mvstoreFile) {
    return new OdbStorage(Optional.ofNullable(mvstoreFile), Optional.empty());
  }

  private OdbStorage(
      final Optional<File> mvstoreFileMaybe,
      final Optional<NodeDeserializer> nodeDeserializer) {
    this.nodeDeserializer = nodeDeserializer;

    if (mvstoreFileMaybe.isPresent()) {
      mvstoreFile = mvstoreFileMaybe.get();
    } else {
      try {
        mvstoreFile = File.createTempFile("mvstore", ".bin");
        mvstoreFile.deleteOnExit();
      } catch (IOException e) {
        throw new RuntimeException("cannot create tmp file for mvstore", e);
      }
    }
    logger.trace("storge file: " + mvstoreFile);
  }

  public void persist(final OdbNode node) throws IOException {
    if (!closed) {
      final long id = node.ref.id;
      getNodesMVMap(id).put(id, nodeSerializer.serialize(node));
    }
  }

  public <A extends Vertex> A readNode(final long id) throws IOException {
    return (A) nodeDeserializer.get().deserialize(getNodesMVMap(id).get(id));
  }

  @Override
  public void close() {
    closed = true;
    logger.info("closing " + getClass().getSimpleName());
    if (mvstore != null) mvstore.close();
  }

  public File getStorageFile() {
    return new File(mvstore.getFileStore().getFileName());
  }

  public void removeNode(final Long id) {
    getNodesMVMap(id).remove(id);
  }

  public Set<Map.Entry<Long, byte[]>> allNodes() {
    final HashSet<Map.Entry<Long, byte[]>> entries = new HashSet<>();
    for (int i=0; i < SHARDS; ++i) {
      entries.addAll(getNodesMVMap(i).entrySet());
    }
    return entries;
  }

  public NodeSerializer getNodeSerializer() {
    return nodeSerializer;
  }

  public MVMap<Long, byte[]> getNodesMVMap(long nodeId) {
    if (mvstore == null) {
      mvstore = initializeMVStore();

    }
//    if (nodesMVMap == null)
//      nodesMVMap = mvstore.openMap("nodes_" + getShardIndex(nodeId));
    return nodesMVMapList.get(getShardIndex(nodeId));
  }

  private int getShardIndex(long nodeId) {
    return (int)(nodeId % SHARDS);
  }

  private MVStore initializeMVStore() {
    final MVStore store = new MVStore.Builder()
        .fileName(mvstoreFile.getAbsolutePath())
        .autoCommitBufferSize(1024 * 4)
        .open();
    for (int i = 0; i < SHARDS; ++i)
      nodesMVMapList.add(store.openMap("nodes_" + i));
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
