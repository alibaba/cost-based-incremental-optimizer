package org.apache.calcite.rel.tvr.utils;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Config.
 */
public class Config {

  Map<String, String> p;

  public Config() {
    p = new LinkedHashMap<>();
  }

  public <T> T getEnum(String key, Class<T> clz, T defaultValue) {
    assert clz.isEnum();
    String value = p.get(key);
    if (value == null) {
      return defaultValue;
    }
    for (Object c : clz.getEnumConstants()) {
      if (c.toString().equalsIgnoreCase(value)) {
        return (T) c;
      }
    }
    return defaultValue;
  }

  public boolean getBool(String key, boolean defaultValue) {
    String value = p.get(key);
    return value == null ? defaultValue : Boolean.parseBoolean(value);
  }

  public double getDouble(String key, double defaultValue) {
    String value = p.get(key);
    return value == null ? defaultValue : Double.parseDouble(value);
  }

  public int getInteger(String key, int defaultValue) {
    String value = p.get(key);
    return value == null ? defaultValue : Integer.parseInt(value);
  }

  public long getLong(String key, long defaultValue) {
    String value = p.get(key);
    return value == null ? defaultValue : Long.parseLong(value);
  }

  public String get(String key, String defaultValue) {
    return p.getOrDefault(key, defaultValue);
  }

  public <T> void set(String key, T value) {
    p.put(key, value.toString());
  }

  public String get(String key) {
    return p.get(key);
}

  public boolean contains(String key) {
    return p.containsKey(key);
  }

  public Set<Map.Entry<String, String>> entrySet() {
    return p.entrySet();
  }
}