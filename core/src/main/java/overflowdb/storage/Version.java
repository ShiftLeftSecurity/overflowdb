package overflowdb.storage;

import java.util.Objects;

public class Version {
  public static Builder newBuilder;

  public static Builder newBuilder() {
//    return new Builder();
    return null;
  }
  
  final int major;
  final int minor;

  public Version(int major, int minor) {
    this.major = major;
    this.minor = minor;
  }

  @Override
  public String toString() {
    return String.format("%s.%s", major, minor);
  }

  public static Version fromString(String s) {
    String[] split = s.split("\\.");
    if (split.length != 2)
      throw new IllegalArgumentException(String.format("illegal version format: %s - must be <int>.<int>, e.g. `5.3`", s));
    int major = Integer.parseInt(split[0]);
    int minor = Integer.parseInt(split[1]);
    return new Version(major, minor);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Version version = (Version) o;
    return major == version.major &&
        minor == version.minor;
  }

  @Override
  public int hashCode() {
    return Objects.hash(major, minor);
  }


  class Builder {
    int major;
    int minor;

    public Builder major(int i) {
      this.major = i;
      return this;
    }

    public Builder minor(int i) {
      this.minor = i;
      return this;
    }

    public Version build() {
      return new Version (major, minor);
    }
  }
}
