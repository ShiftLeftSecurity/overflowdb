package io.shiftleft.overflowdb.storage;

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

public class OdbStorage implements AutoCloseable {
  private final Logger logger = LoggerFactory.getLogger(getClass());
  protected final NodeSerializer nodeSerializer = new NodeSerializer();
  protected final Optional<NodeDeserializer> vertexDeserializer;

  private final File mvstoreFile;
  private MVStore mvstore; // initialized in `getVertexMVMap`
  private MVMap<Long, byte[]> vertexMVMap;
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
      final Optional<NodeDeserializer> vertexDeserializer) {
    this.vertexDeserializer = vertexDeserializer;

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
    logger.debug("on-disk overflow file: " + mvstoreFile);
  }

  public void persist(final OdbNode node) throws IOException {
    if (!closed) {
      final long id = node.ref.id;
      getVertexMVMap().put(id, nodeSerializer.serialize(node));
    }
  }

  public <A extends Vertex> A readVertex(final long id) throws IOException {
    return (A) vertexDeserializer.get().deserialize(getVertexMVMap().get(id));
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
    getVertexMVMap().remove(id);
  }

  public Set<Map.Entry<Long, byte[]>> allVertices() {
    return getVertexMVMap().entrySet();
  }

  public NodeSerializer getNodeSerializer() {
    return nodeSerializer;
  }

  public MVMap<Long, byte[]> getVertexMVMap() {
    if (mvstore == null) {
      mvstore = new MVStore.Builder().fileName(mvstoreFile.getAbsolutePath()).open();
      vertexMVMap = mvstore.openMap("vertices");
    }
    return vertexMVMap;
  }

  public Optional<NodeDeserializer> getVertexDeserializer() {
    return vertexDeserializer;
  }
}
