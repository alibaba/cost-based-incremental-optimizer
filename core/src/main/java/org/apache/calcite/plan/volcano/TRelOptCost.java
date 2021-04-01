package org.apache.calcite.plan.volcano;

import org.apache.calcite.plan.RelOptCost;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TRelOptCost implements RelOptCost {
  RelOptCost floating;
  int earliest;
  final RelOptCost[] fixed;

  public static double[] weights = null;

  TRelOptCost(RelOptCost floating, int earliest, RelOptCost[] fixed) {
    this.floating = floating;
    this.earliest = earliest;
    this.fixed = fixed;
  }

  TRelOptCost(TRelOptCost other) {
    this(other.floating, other.earliest, other.fixed.clone());
  }

  @Override
  public double getRows() {
    return Arrays.stream(fixed).mapToDouble(RelOptCost::getRows)
        .reduce(floating.getRows(), Double::sum);
  }

  @Override
  public double getCpu() {
    return Arrays.stream(fixed).mapToDouble(RelOptCost::getCpu)
        .reduce(floating.getCpu(), Double::sum);
  }

  @Override
  public double getIo() {
    return Arrays.stream(fixed).mapToDouble(RelOptCost::getIo)
        .reduce(floating.getIo(), Double::sum);
  }

  @Override
  public boolean isInfinite() {
    return floating.isInfinite() || Arrays.stream(fixed)
        .anyMatch(RelOptCost::isInfinite);
  }

  public RelOptCost[] fix() {
    RelOptCost[] settled = Arrays.copyOf(fixed, fixed.length);
    settled[earliest] = settled[earliest].plus(floating);
    return settled;
  }

  public RelOptCost[] fix(int time) {
    assert time < fixed.length;
    RelOptCost[] settled = Arrays.copyOf(fixed, fixed.length);
    settled[time] = settled[time].plus(floating);
    return settled;
  }

  public TRelOptCost pointPlus(RelOptCost cost, int i) {
    return new TRelOptCost(this).pointPlusInPlace(cost, i);
  }

  public TRelOptCost pointPlus(RelOptCost cost) {
    return new TRelOptCost(this).pointPlusInPlace(cost);
  }

  public TRelOptCost pointPlusInPlace(RelOptCost cost, int i) {
    fixed[i] = fixed[i].plus(cost);
    return this;
  }

  public TRelOptCost pointPlusInPlace(RelOptCost cost) {
    floating = floating.plus(cost);
    return this;
  }

  private RelOptCost get(int i) {
    RelOptCost cost = fixed[i];
    if (i == earliest) {
      cost = cost.plus(floating);
    }
    return cost;
  }

  @Override
  public boolean equals(RelOptCost other) {
    assert other instanceof TRelOptCost;
    TRelOptCost that = (TRelOptCost) other;
    assert this.fixed.length == that.fixed.length;
    for (int i = 0; i < this.fixed.length; ++i) {
      if (!this.get(i).equals(that.get(i))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean isEqWithEpsilon(RelOptCost other) {
    assert other instanceof TRelOptCost;
    TRelOptCost that = (TRelOptCost) other;
    assert this.fixed.length == that.fixed.length;
    for (int i = 0; i < this.fixed.length; ++i) {
      if (!this.get(i).isEqWithEpsilon(that.get(i))) {
        return false;
      }
    }
    return true;
  }

  public Double weightedCost() {
    if (weights == null) {
      return null;
    }
    assert weights.length == fixed.length;
    double weighedCost = 0d;
    for (int i = this.fixed.length - 1; i >= 0; --i) {
      double costDouble = getCost(get(i));
      weighedCost += costDouble * weights[i];
    }
    return weighedCost;
  }

  @Override
  public boolean isLe(RelOptCost cost) {
    assert cost instanceof TRelOptCost;
    TRelOptCost that = (TRelOptCost) cost;
    if (this.isInfinite()) {
      return false;
    }
    if (that.isInfinite()) {
      return true;
    }
    assert this.fixed.length == that.fixed.length;

    if (weights != null) {
      return this.weightedCost() <= that.weightedCost();
    }

    for (int i = this.fixed.length - 1; i >= 0; --i) {
      if (this.get(i).isLt(that.get(i))) {
        return true;
      } else if (that.get(i).isLt(this.get(i))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean isLt(RelOptCost cost) {
    assert cost instanceof TRelOptCost;
    TRelOptCost that = (TRelOptCost) cost;
    if (this.isInfinite()) {
      return false;
    }
    if (that.isInfinite()) {
      return true;
    }
    assert this.fixed.length == that.fixed.length;

    if (weights != null) {
      return this.weightedCost() < that.weightedCost();
    }

    for (int i = this.fixed.length - 1; i >= 0; --i) {
      if (this.get(i).isLt(that.get(i))) {
        return true;
      } else if (that.get(i).isLt(this.get(i))) {
        return false;
      }
    }
    return false;
  }

  @Override
  public TRelOptCost plus(RelOptCost cost) {
    assert cost instanceof TRelOptCost;
    TRelOptCost that = (TRelOptCost) cost;
    assert this.fixed.length == that.fixed.length;
    RelOptCost[] newFixed = new RelOptCost[this.fixed.length];
    for (int i = 0; i < this.fixed.length; ++i) {
      newFixed[i] = this.fixed[i].plus(that.fixed[i]);
    }
    return new TRelOptCost(this.floating.plus(that.floating),
        Math.max(this.earliest, that.earliest), newFixed);
  }

  public TRelOptCost plusInPlace(RelOptCost cost) {
    assert cost instanceof TRelOptCost;
    TRelOptCost that = (TRelOptCost) cost;
    this.floating = this.floating.plus(that.floating);
    assert this.fixed.length == that.fixed.length;
    for (int i = 0; i < this.fixed.length; ++i) {
      this.fixed[i] = this.fixed[i].plus(that.fixed[i]);
    }
    this.earliest = Math.max(this.earliest, that.earliest);
    return this;
  }

  @Override
  public TRelOptCost minus(RelOptCost cost) {
    assert cost instanceof TRelOptCost;
    TRelOptCost that = (TRelOptCost) cost;
    assert this.fixed.length == that.fixed.length;
    RelOptCost[] newFixed = new RelOptCost[this.fixed.length];
    for (int i = 0; i < this.fixed.length; ++i) {
      newFixed[i] = this.fixed[i].minus(that.fixed[i]);
    }
    return new TRelOptCost(this.floating.minus(that.floating),
        Math.max(this.earliest, that.earliest), newFixed);
  }

  @Override
  public TRelOptCost multiplyBy(double factor) {
    RelOptCost[] newFixed = new RelOptCost[this.fixed.length];
    for (int i = 0; i < this.fixed.length; ++i) {
      newFixed[i] = this.fixed[i].multiplyBy(factor);
    }
    return new TRelOptCost(floating.multiplyBy(factor), earliest, newFixed);
  }

  @Override
  public double divideBy(RelOptCost cost) {
    assert cost instanceof TRelOptCost;
    TRelOptCost that = (TRelOptCost) cost;
    return this.sum().divideBy(that.sum());
  }

  private RelOptCost sum() {
    return Arrays.stream(fixed).reduce(floating, RelOptCost::plus);
  }

  @Override
  public String toString() {
    return IntStream.range(0, fixed.length)
        .mapToObj(i -> getCost(get(i)))
        .collect(Collectors.toList()) + " <" + floating + "@" + earliest + "+"
        + Arrays.toString(fixed) + ">";
  }

  public static double getCost(RelOptCost cost) {
    // use row count to represent cost for now, instead of a weighted row count / cpu / io cost
    return cost.getRows();
  }

}
