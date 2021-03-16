package org.apache.calcite.plan.tvr;

public abstract class TvrSetSemantics extends TvrSemantics {

  protected TvrSetSemantics(TvrVersion fromVersion, TvrVersion toVersion) {
    super(fromVersion, toVersion);
  }

}
