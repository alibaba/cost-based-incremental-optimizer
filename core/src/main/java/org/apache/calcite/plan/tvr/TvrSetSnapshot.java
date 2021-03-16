package org.apache.calcite.plan.tvr;

public class TvrSetSnapshot extends TvrSetSemantics {

  public TvrSetSnapshot(TvrVersion atVersion) {
    super(TvrVersion.MIN, atVersion);
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof TvrSetSnapshot && super.equals(obj);
  }

  @Override
  public String toString() {
    return "SetSnapshot@(" + toVersion + ")";
  }

  @Override
  public TvrSetSnapshot copy(TvrVersion from, TvrVersion to) {
    assert from.equals(TvrVersion.MIN);
    return new TvrSetSnapshot(to);
  }
}
