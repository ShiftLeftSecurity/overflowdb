package io.shiftleft.overflowdb.storage;

import io.shiftleft.overflowdb.NodeRef;
import io.shiftleft.overflowdb.OdbNode;
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
  protected final NodeSerializer nodeSerializer = new NodeSerializer();
  protected final Optional<NodeDeserializer> nodeDeserializer;

  private final File mvstoreFile;
  private MVStore mvstore; // initialized in `getNodesMVMap`
  private MVMap<Long, byte[]> nodesMVMap;
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
      getNodesMVMap().put(id, nodeSerializer.serialize(node));
    }
  }

  public <A extends Vertex> A readNode(final long id) throws IOException {
    return (A) nodeDeserializer.get().deserialize(getNodesMVMap().get(id));
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
    final MVStore store = new MVStore.Builder().fileName(mvstoreFile.getAbsolutePath()).open();
    return store;
  }

  private Map<String, String> getIndexNameMap(MVStore store) {
    store.getMapNames().stream().forEach(name -> System.out.println("name: " + name));
    return store
        .getMapNames()
        .stream()
        .filter(s -> s.startsWith(INDEX_PREFIX))
        .map(s -> {System.out.println("filtered name: " + s); return s;})
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
    System.out.println("Opening index: " + indexName);
    assert indexName != null && !indexName.isEmpty();
    final String mapName = getIndexMapName(indexName);
    System.out.println("Opening index nvmap: " + mapName);
    MVMap<Object, long[]> indexMVMap = mvstore.openMap(mapName);
    return indexMVMap;
  }

  private String getIndexMapName(String indexName) {
    return INDEX_PREFIX + indexName;
  }


  public Optional<NodeDeserializer> getNodeDeserializer() {
    return nodeDeserializer;
  }

  public void clearIndices() {
    getIndexNames().forEach(this::clearIndex);
  }

  private void clearIndex(String indexName) {
    System.out.println("clearing index: " + indexName);
    openIndex(indexName).clear();
  }

  public void saveIndex(String propertyName, Map<Object, Set<NodeRef>> indexMap) {
    final MVMap<Object, long[]> indexStore = openIndex(propertyName);
    indexMap.entrySet().parallelStream().forEach(entry -> {
      final Object propertyValue = entry.getKey();
      final Set<NodeRef> nodeRefs = entry.getValue();
      indexStore.put(propertyValue, nodeRefs.stream().mapToLong(nodeRef -> nodeRef.id).toArray());
    });
  }
}
