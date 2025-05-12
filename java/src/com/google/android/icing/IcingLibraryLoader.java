package com.google.android.icing;

/**
 * This loader is used to load libicing.so native library. This is the default loader to use for
 * Icing library.
 */
final class IcingLibraryLoader {
  public static void loadLibrary() {
    System.loadLibrary("icing");
  }

  private IcingLibraryLoader() {}
}
