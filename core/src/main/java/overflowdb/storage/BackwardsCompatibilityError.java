package overflowdb.storage;

public class BackwardsCompatibilityError extends RuntimeException {
  public BackwardsCompatibilityError(String msg) {
    super(msg);
  }
}
