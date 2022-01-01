package com.zmh.fastlog.config;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import java.nio.file.Path;
import java.nio.file.Paths;

@Value
@Builder
public class CompressedFilesConfig {

  Path folder;

  Long maxSizeBytes;

  /**
   * The configuration builder.
   */
  public static class CompressedFilesConfigBuilder {

    /**
     * Sets the folder's name where compressed filess will be placed.
     *
     * @param value the new value
     *
     * @return the builder object for chain calls
     */
    public CompressedFilesConfigBuilder folder (@NonNull String value) {
      return folder(Paths.get(value));
    }

    /**
     * Sets the folder's path where compressed filess will be placed.
     *
     * @param value the new value
     *
     * @return the builder object for chain calls
     */
    public CompressedFilesConfigBuilder folder (@NonNull Path value) {
      folder = value;
      return this;
    }

    /**
     * Sets the maximum compressed file size in bytes.
     *
     * @param value the new value
     *
     * @return the builder object for chain calls
     */
    public CompressedFilesConfigBuilder maxSizeBytes (long value) {
      maxSizeBytes = value;
      return this;
    }
  }
}
