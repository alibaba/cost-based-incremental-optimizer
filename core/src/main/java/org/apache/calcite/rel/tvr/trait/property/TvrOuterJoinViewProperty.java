package org.apache.calcite.rel.tvr.trait.property;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.gson.*;
import org.apache.calcite.plan.volcano.TvrProperty;

import java.lang.reflect.Type;
import java.util.*;

/**
 * Related information used for outer join view rules. (Efficient Maintenance of
 * Materialized Outer-Join Views, ICDE'07)
 *
 * This tvr property is used as a self loop on tvr. It is considered as a commit
 * message, i.e. all {@link TvrUpdateOneTableProperty} linked for this tvr has
 * been fully connected.
 */
public class TvrOuterJoinViewProperty extends TvrProperty {

  // List of possible terms (combination of source tables) in the result
  // The list is sorted so that this.equals() works properly
  private List<LinkedHashSet<Integer>> terms;

  // Trace the provenance of each column, i.e. which source table the column
  // comes from. Empty set means this column is a constant.
  private List<LinkedHashSet<Integer>> nonNullTermTables;

  // Ordered (pre-order in the rel tree) list of changing tables
  private LinkedHashSet<Integer> changingTables;

  public TvrOuterJoinViewProperty(List<LinkedHashSet<Integer>> terms,
      List<LinkedHashSet<Integer>> nonNullTermTables,
      Set<Integer> changingTables) {
    // Sort the terms list so that this.equals() works properly
    this.terms = new ArrayList<>(terms);
    this.terms.sort((set1, set2) -> {
      if (set1.size() != set2.size()) {
        return set1.size() - set2.size();
      }
      // Assume both sets are sorted already, so compare each item one by one
      Iterator<Integer> iter1 = set1.iterator();
      Iterator<Integer> iter2 = set2.iterator();
      while (iter1.hasNext()) {
        int n = Integer.compare(iter1.next(), iter2.next());
        if (n != 0) {
          return n;
        }
      }
      return 0;
    });

    this.nonNullTermTables = nonNullTermTables;
    this.changingTables = new LinkedHashSet<>(changingTables);
  }

  public List<LinkedHashSet<Integer>> getTerms() {
    return this.terms;
  }

  public List<LinkedHashSet<Integer>> getNonNullTermTables() {
    return this.nonNullTermTables;
  }

  public LinkedHashSet<Integer> allChangingTermTables() {
    return changingTables;
  }

  @Override
  public String toString() {
    return "OJV:" + changingTables.toString();
  }

  public String toFullString() {
    return terms.toString() + " -- " + changingTables.toString() + " -- "
        + nonNullTermTables.toString();
  }

  @Override
  public int hashCode() {
    return Objects.hash(terms, nonNullTermTables, changingTables);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof TvrOuterJoinViewProperty)) {
      return false;
    }
    TvrOuterJoinViewProperty o = (TvrOuterJoinViewProperty) other;
    return Objects.equals(terms, o.terms) && Objects
        .equals(nonNullTermTables, o.nonNullTermTables) && Objects
        .equals(changingTables, o.changingTables);
  }

  public static LinkedHashSet<Integer> getOrderedTableSet(
      Collection<Integer> term) {
    List<Integer> list = new ArrayList<>(term);
    list.sort(Comparator.naturalOrder());
    return new LinkedHashSet<>(list);
  }

  public static class TvrOuterJoinViewPropertySerde
      implements JsonDeserializer<TvrOuterJoinViewProperty>,
      JsonSerializer<TvrOuterJoinViewProperty> {

    public static TvrOuterJoinViewPropertySerde
        INSTANCE = new TvrOuterJoinViewPropertySerde();

    @Override
    public TvrOuterJoinViewProperty deserialize(JsonElement json, Type typeOfT,
        JsonDeserializationContext context) throws JsonParseException {
      return new TvrOuterJoinViewProperty(ImmutableList.of(),
          ImmutableList.of(), ImmutableSet.of());
    }

    @Override
    public JsonElement serialize(TvrOuterJoinViewProperty src, Type typeOfSrc,
        JsonSerializationContext context) {
      return new JsonObject();
    }
  }

}
