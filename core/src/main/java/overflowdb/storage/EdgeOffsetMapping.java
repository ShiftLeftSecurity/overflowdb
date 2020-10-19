package overflowdb.storage;

import java.util.Map;

// TODO use, doc
public class EdgeOffsetMapping {
  public final int nodeTypeId;
  private final Map<Integer, Integer> storageOffsetIdxToCurrentSchemaIdx;

  public EdgeOffsetMapping(int nodeTypeId, Map<Integer, Integer> storageOffsetIdxToCurrentSchemaIdx) {
    this.nodeTypeId = nodeTypeId;
    this.storageOffsetIdxToCurrentSchemaIdx = storageOffsetIdxToCurrentSchemaIdx;
  }

  public int currentIdxForStorageIndex(int idxFromStorage) {
    if (!storageOffsetIdxToCurrentSchemaIdx.containsKey(idxFromStorage)) {
      // TODO better error msg with more context
      throw new BackwardsCompatibilityException("unable to translate TODO foo");
    } else {
      return storageOffsetIdxToCurrentSchemaIdx.get(idxFromStorage);
    }
  }

  public void addMapping(int currentEdgeOffset, int offsetFromStorage) {
    storageOffsetIdxToCurrentSchemaIdx.put(offsetFromStorage, currentEdgeOffset);
  }
}
