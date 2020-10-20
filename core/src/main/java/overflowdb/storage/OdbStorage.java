package overflowdb.storage;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import overflowdb.Direction;
import overflowdb.Node;
import overflowdb.NodeDb;
import overflowdb.NodeLayoutInformation;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OdbStorage implements AutoCloseable {
  private static final String INDEX_PREFIX = "index_";
  private static final String EDGE_OFFSET_PREFIX = "schema_edge_offsets__";

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
        .compress()
        .open();
    return store;
  }

  public Optional<NodeDeserializer> getNodeDeserializer() {
    return nodeDeserializer;
  }

  public Set<String> getIndexNames() {
    return mvstore
        .getMapNames()
        .stream()
        .filter(s -> s.startsWith(INDEX_PREFIX))
        .map(this::removeIndexPrefix)
        .collect(Collectors.toSet());
  }

  private String removeIndexPrefix(String mapName) {
    if (!mapName.startsWith(INDEX_PREFIX))
      throw new AssertionError(String.format("attempted to treat %s as an index table, but it doesn't have the correct `%s` prefix!", mapName, INDEX_PREFIX));
    return mapName.substring(INDEX_PREFIX.length());
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

  //  schema_edgeoffsets_$nodeTypeId_$direction_$edgeType -> Map(CDG -> 5, REF -> 6, ...)
  // TODO call this when saving the graph
  // TODO move, doc
  public void persistEdgeOffsets(Stream<NodeLayoutInformation> nodeLayoutInfos) {
    nodeLayoutInfos.forEach(layout -> {
      final MVMap<String, Integer> inEdgeOffsetsMap = edgeOffsetsMap(layout.labelId, Direction.IN);
      for (String label : layout.allowedInEdgeLabels()) {
        inEdgeOffsetsMap.put(label, layout.inEdgeToOffsetPosition(label));
      }

      final MVMap<String, Integer> outEdgeOffsetsMap = edgeOffsetsMap(layout.labelId, Direction.OUT);
      for (String label : layout.allowedOutEdgeLabels()) {
        outEdgeOffsetsMap.put(label, layout.outEdgeToOffsetPosition(label));
      }
    });
    flush();
  }

  // TODO call this when opening the graph, create EdgeOffsetMappings
  // TODO move, doc
  public Set<EdgeOffset> edgeOffsets() {
    return mvstore
        .getMapNames()
        .stream()
        .filter(s -> s.startsWith(EDGE_OFFSET_PREFIX))
        .flatMap(mapName -> {
          // TODO do some validation
          String[] parts = mapName.split("__");
          int nodeId = Integer.parseInt(parts[1]);
          Direction direction = Direction.valueOf(parts[2]);

          final MVMap<String, Integer> edgeOffsetMap = mvstore.openMap(mapName);
          Set<EdgeOffset> offsets = new HashSet<>(edgeOffsetMap.size());
          edgeOffsetMap.forEach((edgeLabel, offset) -> offsets.add(new EdgeOffset(nodeId, direction, edgeLabel, offset)));
          return offsets.stream();
        })
        .collect(Collectors.toSet());
  }

  // TODO move, doc
  private String edgeOffsetMapName(int nodeTypeId, Direction direction) {
    return String.format("%s%d__%s", EDGE_OFFSET_PREFIX, nodeTypeId, direction.toString());
  }

  // TODO move, doc
  private MVMap<String, Integer> edgeOffsetsMap(int nodeTypeId, Direction direction) {
    if (mvstore == null) {
      mvstore = initializeMVStore();
    }
    return mvstore.openMap(edgeOffsetMapName(nodeTypeId, direction));
  }
}
