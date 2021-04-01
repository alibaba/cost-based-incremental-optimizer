package org.apache.calcite.rel.tvr.utils;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

import java.util.function.IntUnaryOperator;

public class RexInputRefReplacer extends RexShuttle {

  private final RexBuilder builder;
  private IntUnaryOperator mapper;

  public RexInputRefReplacer(RexBuilder builder, IntUnaryOperator mapper) {
    this.builder = builder;
    this.mapper = mapper;
  }

  @Override
  public RexNode visitInputRef(RexInputRef inputRef) {
    return builder.makeInputRef(inputRef.getType(),
        mapper.applyAsInt(inputRef.getIndex()));
  }
}
