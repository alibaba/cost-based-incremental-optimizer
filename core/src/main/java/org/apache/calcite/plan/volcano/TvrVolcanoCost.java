package org.apache.calcite.plan.volcano;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptUtil;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Function;

/**
 * OdpsRelOptCost implementation.
 */
public class TvrVolcanoCost implements RelOptCost {

  public static final TvrVolcanoCostFactory FACTORY = new TvrVolcanoCostFactory();

  //~ Instance fields --------------------------------------------------------

  final BigDecimal cpu;
  final BigDecimal io;
  final BigDecimal rowCount;
  final BigDecimal memory;
  final BigDecimal network;
  final BigDecimal cost;

  /**
   * IO weight to CPU.
   */
  public static final double IO_WEIGHT = 1d;

  /**
   * Memory weight to CPU is usually a piecewise linear function.
   */
  public static final Function<Double, Double> MEMORY_FACTOR =
      memory -> {
        double level1 = 1024d * 1024 * 1024;      // 1G, factor = .1
        double level2 = 4d * 1024 * 1024 * 1024;  // 4G, factor = 1, other = 10
        double cost = 0;
        if (memory > level2) {
          cost += (memory - level2) * 10;
          memory = level2;
        }
        if (memory > level1) {
          cost += (memory - level1) * 1;
          memory = level1;
        }
        if (memory > 0) {
          cost += memory * .1;
        }
        return cost;
      };

  /**
   * Network wight to IO.
   */
  public static final double NETWORK_TO_IO_RATIO = 4d;


  //~ Static fields/initializers ---------------------------------------------

  static final TvrVolcanoCost INFINITY =
      new TvrVolcanoCost(
          null,
          null,
          null,
          null,
          null) {
        public String toString() {
          return "{inf}";
        }
      };

  static final TvrVolcanoCost HUGE =
      new TvrVolcanoCost(
          Double.MAX_VALUE,
          Double.MAX_VALUE,
          Double.MAX_VALUE,
          Double.MAX_VALUE,
          Double.MAX_VALUE) {
        public String toString() {
          return "{huge}";
        }
      };

  static final TvrVolcanoCost ZERO =
      new TvrVolcanoCost(0.0, 0.0, 0.0, 0.0, 0.0) {
        public String toString() {
          return "{0}";
        }
      };

  static final TvrVolcanoCost TINY =
      new TvrVolcanoCost(1.0, 1.0, 1.0, 1.0, 0.0) {
        public String toString() {
          return "{tiny}";
        }
      };


  //~ Constructors -----------------------------------------------------------

  TvrVolcanoCost(double rowCount, double cpu, double io, double avgMemory,
                 double networkIO) {
    if (rowCount == Double.POSITIVE_INFINITY) {
      this.rowCount = null;
    } else {
      this.rowCount = BigDecimal.valueOf(rowCount);
    }

    if (cpu == Double.POSITIVE_INFINITY) {
      this.cpu = null;
    } else {
      this.cpu = BigDecimal.valueOf(cpu);
    }

    if (io == Double.POSITIVE_INFINITY) {
      this.io = null;
    } else {
      this.io = BigDecimal.valueOf(io);
    }

    if (avgMemory == Double.POSITIVE_INFINITY) {
      this.memory = null;
    } else {
      this.memory = BigDecimal.valueOf(avgMemory);
    }

    if (networkIO == Double.POSITIVE_INFINITY) {
      this.network = null;
    } else {
      this.network = BigDecimal.valueOf(networkIO);
    }

    double mem = MEMORY_FACTOR.apply(avgMemory);
    double cost = cpu
        + io * IO_WEIGHT
        + networkIO * NETWORK_TO_IO_RATIO * IO_WEIGHT
        + mem;
    if (cost == Double.POSITIVE_INFINITY) {
      this.cost = null;
    } else {
      this.cost = BigDecimal.valueOf(cpu)
          .add(BigDecimal.valueOf(io).multiply(BigDecimal.valueOf(IO_WEIGHT)))
          .add(BigDecimal.valueOf(networkIO)
              .multiply(BigDecimal.valueOf(NETWORK_TO_IO_RATIO))
              .multiply(BigDecimal.valueOf(IO_WEIGHT)))
          .add(BigDecimal.valueOf(mem));
    }
  }

  TvrVolcanoCost(BigDecimal rowCount, BigDecimal cpu, BigDecimal io, BigDecimal avgMemory,
                 BigDecimal networkIO) {
    this.rowCount = rowCount;
    this.cpu = cpu;
    this.io = io;
    this.memory = avgMemory;
    this.network = networkIO;
    Double mem = avgMemory == null ? null : MEMORY_FACTOR.apply(avgMemory.doubleValue());
    if (cpu == null || io == null || networkIO == null || avgMemory == null || mem == Double.POSITIVE_INFINITY) {
      this.cost = null;
    } else {
      this.cost = cpu.add(io.multiply(BigDecimal.valueOf(IO_WEIGHT)))
          .add(networkIO.multiply(BigDecimal.valueOf(NETWORK_TO_IO_RATIO))
              .multiply(BigDecimal.valueOf(IO_WEIGHT)))
          .add(BigDecimal.valueOf(mem));
    }
  }

  //~ Methods ----------------------------------------------------------------

  public BigDecimal getCost() {
    return this.cost;
  }

  public double getCpu() {
    return cpu.doubleValue();
  }

  public double getIo() {
    return io.doubleValue();
  }

  public double getRows() {
    return rowCount.doubleValue();
  }

  public boolean isInfinite() {
    return (this == INFINITY)
        || (this.cost == null)
        || (this.rowCount == null)
        || (this.cpu == null)
        || (this.io == null)
        || (this.memory == null)
        || (this.network == null);
  }

  public boolean isLe(RelOptCost other) {
    TvrVolcanoCost that = (TvrVolcanoCost) other;
    if (this == that) {
      return true;
    }

    int cmp = compareTo(this.cost, that.cost);
    return cmp == -1 || cmp == 0;
  }

  private int compareTo(BigDecimal c1, BigDecimal c2) {
    if (c1 == null && c2 == null) {
      return 0;
    } else if (c1 == null) {
      return 1;
    } else if (c2 == null) {
      return -1;
    }
    return c1.compareTo(c2);
  }

  public boolean isLt(RelOptCost other) {
    TvrVolcanoCost that = (TvrVolcanoCost) other;

    if (this == INFINITY && that == INFINITY) {
      return false;
    } else if (this == INFINITY) {
      return false;
    } else if (that == INFINITY) {
      return true;
    } else {
      int cmp = compareTo(this.cost, that.cost);
      return cmp == -1;
    }
  }

  @Override public int hashCode() {
    return Objects.hash(rowCount, cpu, io, memory, network);
  }

  public boolean equals(RelOptCost other) {
    return this == other
        || other instanceof TvrVolcanoCost
        && compareTo(this.rowCount, ((TvrVolcanoCost) other).rowCount) == 0
        && compareTo(this.cpu, ((TvrVolcanoCost) other).cpu) == 0
        && compareTo(this.io, ((TvrVolcanoCost) other).io) == 0
        && compareTo(this.memory, ((TvrVolcanoCost) other).memory) == 0
        && compareTo(this.network, ((TvrVolcanoCost) other).network) == 0;
  }

  public boolean isEqWithEpsilon(RelOptCost other) {
    if (!(other instanceof TvrVolcanoCost)) {
      return false;
    }
    TvrVolcanoCost that = (TvrVolcanoCost) other;
    if (this == that) {
      return true;
    }

    if (this.cost == null && that.cost == null) {
      return true;
    } else if (this.cost == null) {
      return false;
    } else if (that.cost == null) {
      return false;
    }

    return (this == that)
        || Math.abs(this.cost.subtract(that.cost).doubleValue()) < RelOptUtil.EPSILON;
  }

  private BigDecimal minus(BigDecimal b1, BigDecimal b2) {
    if (b1 == null || b2 == null) {
      return null;
    }
    return b1.subtract(b2);
  }
  public RelOptCost minus(RelOptCost other) {
    if (this == INFINITY || other == INFINITY) {
      return INFINITY;
    }
    if (other == ZERO) {
      return this;
    }
    TvrVolcanoCost that = (TvrVolcanoCost) other;
    return new TvrVolcanoCost(
        minus(this.rowCount, that.rowCount),
        minus(this.cpu, that.cpu),
        minus(this.io, that.io),
        minus(this.memory, that.memory),
        minus(this.network, that.network));
  }

  private BigDecimal multiplyBy(BigDecimal b1, double factor) {
    if (b1 == null) {
      return null;
    }
    return b1.multiply(BigDecimal.valueOf(factor));
  }

  public RelOptCost multiplyBy(double factor) {
    if (this == INFINITY) {
      return this;
    }
    if (this == ZERO) {
      return this;
    }
    return new TvrVolcanoCost(
        multiplyBy(this.rowCount, factor),
        multiplyBy(this.cpu, factor),
        multiplyBy(this.io, factor),
        multiplyBy(this.memory, factor),
        multiplyBy(this.network, factor));
  }

  private BigDecimal divideBy(BigDecimal b1, BigDecimal b2) {
    if (b1 == null || b2 == null) {
      return null;
    }
    return b1.divide(b2);
  }

  public double divideBy(RelOptCost cost) {
    // Compute the geometric average of the ratios of all of the factors
    // which are non-zero and finite.
    TvrVolcanoCost that = (TvrVolcanoCost) cost;
    BigDecimal d = BigDecimal.valueOf(1);
    double n = 0;
    BigDecimal tmp = divideBy(this.rowCount, that.rowCount);
    if (tmp != null) {
      d = tmp.multiply(d);
      ++n;
    }
    tmp = divideBy(this.cpu, that.cpu);
    if (tmp != null) {
      d = tmp.multiply(d);
      ++n;
    }

    tmp = divideBy(this.io, that.io);
    if (tmp != null) {
      d = tmp.multiply(d);
      ++n;
    }

    if (n == 0) {
      return 1.0;
    }
    return Math.pow(d.doubleValue(), 1 / n);
  }

  private BigDecimal plus(BigDecimal b1, BigDecimal b2) {
    if (b1 == null || b2 == null) {
      return null;
    }
    return b1.add(b2);
  }

  public RelOptCost plus(RelOptCost other) {
    TvrVolcanoCost that = (TvrVolcanoCost) other;
    if (this == INFINITY || that == INFINITY) {
      return INFINITY;
    }
    if (this == ZERO) {
      return that;
    }
    if (that == ZERO) {
      return this;
    }
    return new TvrVolcanoCost(
        plus(this.rowCount, that.rowCount),
        plus(this.cpu, that.cpu),
        plus(this.io, that.io),
        plus(this.memory, that.memory),
        plus(this.network, that.network));
  }

  public String toString() {
    return "{" + formatCostScientific(rowCount) + " rows, "
        + formatCostScientific(cpu) + " cpu, " + formatCostScientific(io)
        + " io, " + formatCostScientific(memory) + " memory, "
        + formatCostScientific(network) + " network}";
  }

  public static String formatCostScientific(BigDecimal costNumber) {
    if (costNumber == null) {
      return "infinity";
    }
    DecimalFormat formatter =
        (DecimalFormat) DecimalFormat.getInstance(Locale.ROOT);
    formatter.applyPattern("#.##############################E0");
    return formatter.format(costNumber);
  }

  /**
   * Implementation of {@link RelOptCostFactory}
   * that creates {@link VolcanoCost}s.
   */
  public static class TvrVolcanoCostFactory implements RelOptCostFactory {
    public RelOptCost makeCost(double dRows, double dCpu, double dIo,
        double dMemory, double dNetworkIO) {
      return new TvrVolcanoCost(dRows, dCpu, dIo, dMemory, dNetworkIO);
    }

    public RelOptCost makeCost(double dRows, double dCpu, double dIo,
        double dNetwork) {
      return new TvrVolcanoCost(dRows, dCpu, dIo, dNetwork, 0);
    }

    public RelOptCost makeCost(double dRows, double dCpu, double dIo) {
      return new TvrVolcanoCost(dRows, dCpu, dIo, 0, 0);
    }

    public RelOptCost makeHugeCost() {
      return TvrVolcanoCost.HUGE;
    }

    public RelOptCost makeInfiniteCost() {
      return TvrVolcanoCost.INFINITY;
    }

    public RelOptCost makeTinyCost() {
      return TvrVolcanoCost.TINY;
    }

    public RelOptCost makeZeroCost() {
      return TvrVolcanoCost.ZERO;
    }
  }
}
