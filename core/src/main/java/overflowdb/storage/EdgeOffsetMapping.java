package overflowdb.storage;

import java.util.Map;

public class EdgeOffsetMapping {
  public final int forNodeLabelId;
  public final String forNodeLabel;
  public final int currentAllowedEdgeCount;
  private final Map<Integer, Integer> storageOffsetIdxToCurrentSchemaIdx;

  public EdgeOffsetMapping(int forNodeLabelId, String forNodeLabel, int currentAllowedEdgeCount, Map<Integer, Integer> storageOffsetIdxToCurrentSchemaIdx) {
    this.forNodeLabelId = forNodeLabelId;
    this.forNodeLabel = forNodeLabel;
    this.currentAllowedEdgeCount = currentAllowedEdgeCount;
    this.storageOffsetIdxToCurrentSchemaIdx = storageOffsetIdxToCurrentSchemaIdx;
  }

  public int currentIdxForStorageIndex(int idxFromStorage) {
    if (!storageOffsetIdxToCurrentSchemaIdx.containsKey(idxFromStorage)) {
      // TODO better error
      throw new BackwardsCompatibilityException("unable to translate TODO foo");
    } else {
      return storageOffsetIdxToCurrentSchemaIdx.get(idxFromStorage);
    }
  }

}
