package org.apache.calcite.plan.volcano;

public abstract class TvrVisitor {

  private TvrMetaSet root;

  public void visit(TvrMetaSet tvr, TvrProperty propertyEdge,
      TvrMetaSet parent) {
    tvr.accept(this);
  }

  public TvrMetaSet go(TvrMetaSet p) {
    this.root = p;
    visit(p, null, null);
    return root;
  }
}
