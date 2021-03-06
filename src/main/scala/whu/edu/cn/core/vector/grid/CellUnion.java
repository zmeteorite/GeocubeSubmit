package whu.edu.cn.core.vector.grid;

import com.google.common.collect.Lists;
import org.locationtech.jts.geom.Point;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * An CellUnion is a region consisting of cells of various sizes. Typically a
 * cell union is used to approximate some other shape. There is a tradeoff
 * between the accuracy of the approximation and how many cells are used. Unlike
 * polygons, cells have a fixed hierarchical structure. This makes them more
 * suitable for optimizations based on preprocessing.
 */
public strictfp class CellUnion implements Iterable<CellId> {

    /**
     * The CellIds that form the Union
     */
    private ArrayList<CellId> cellIds = new ArrayList<CellId>();

    public CellUnion() {
    }

    public void initFromCellIds(ArrayList<CellId> cellIds) {
        initRawCellIds(cellIds);
        normalize();
    }

    /**
     * Populates a cell union with the given S2CellIds or 64-bit cells ids, and
     * then calls Normalize(). The InitSwap() version takes ownership of the
     * vector data without copying and clears the given vector. These methods may
     * be called multiple times.
     */
    public void initFromIds(ArrayList<Long> cellIds) {
        initRawIds(cellIds);
        normalize();
    }

    public void initSwap(ArrayList<CellId> cellIds) {
        initRawSwap(cellIds);
        normalize();
    }

    public void initRawCellIds(ArrayList<CellId> cellIds) {
        this.cellIds = cellIds;
    }

    public void initRawIds(ArrayList<Long> cellIds) {
        int size = cellIds.size();
        this.cellIds = new ArrayList<>(size);
        for (Long id : cellIds) {
            this.cellIds.add(new CellId(id));
        }
    }

    /**
     * Like Init(), but does not call Normalize(). The cell union *must* be
     * normalized before doing any calculations with it, so it is the caller's
     * responsibility to make sure that the input is normalized. This method is
     * useful when converting cell unions to another representation and back.
     * These methods may be called multiple times.
     */
    public void initRawSwap(ArrayList<CellId> cellIds) {
        this.cellIds = new ArrayList<>(cellIds);
        cellIds.clear();
    }

    public int size() {
        return cellIds.size();
    }

    /**
     * Convenience methods for accessing the individual cell ids.
     */
    public CellId cellId(int i) {
        return cellIds.get(i);
    }

    /**
     * Enable iteration over the union's cells.
     */
    @Override
    public Iterator<CellId> iterator() {
        return cellIds.iterator();
    }

    /**
     * Direct access to the underlying vector for iteration .
     */
    public ArrayList<CellId> cellIds() {
        return cellIds;
    }

    /**
     * Replaces "output" with an expanded version of the cell union where any
     * cells whose level is less than "min_level" or where (level - min_level) is
     * not a multiple of "level_mod" are replaced by their children, until either
     * both of these conditions are satisfied or the maximum level is reached.
     * <p>
     * This method allows a covering generated by RegionCoverer using
     * min_level() or level_mod() constraints to be stored as a normalized cell
     * union (which allows various geometric computations to be done) and then
     * converted back to the original list of cell ids that satisfies the desired
     * constraints.
     */
    public void denormalize(int minLevel, int levelMod, ArrayList<CellId> output) {
        output.clear();
        output.ensureCapacity(size());
        for (CellId id : this) {
            int level = id.level();
            int newLevel = Math.max(minLevel, level);
            if (levelMod > 1) {
                // Round up so that (new_level - min_level) is a multiple of level_mod.
                // (Note that CellId::kMaxLevel is a multiple of 1, 2, and 3.)
                newLevel += (CellId.MAX_LEVEL - (newLevel - minLevel)) % levelMod;
                newLevel = Math.min(CellId.MAX_LEVEL, newLevel);
            }
            if (newLevel == level) {
                output.add(id);
            } else {
                CellId end = id.childEnd(newLevel);
                for (id = id.childBegin(newLevel); !id.equals(end); id = id.next()) {
                    output.add(id);
                }
            }
        }
    }

    /**
     * If there are more than "excess" elements of the cell_ids() vector that are
     * allocated but unused, reallocate the array to eliminate the excess space.
     * This reduces memory usage when many cell unions need to be held in memory
     * at once.
     */
    public void pack() {
        cellIds.trimToSize();
    }

    /**
     * Return true if the cell union contains the given cell id. Containment is
     * defined with respect to regions, e.g. a cell contains its 4 children. This
     * is a fast operation (logarithmic in the size of the cell union).
     */
    public boolean contains(CellId id) {
        // This function requires that Normalize has been called first.
        //
        // This is an exact test. Each cell occupies a linear span of the
        // space-filling curve, and the cell id is simply the position at the center
        // of this span. The cell union ids are sorted in increasing order along
        // the space-filling curve. So we simply find the pair of cell ids that
        // surround the given cell id (using binary search). There is containment
        // if and only if one of these two cell ids contains this cell.

        int pos = Collections.binarySearch(cellIds, id);
        if (pos < 0) {
            pos = -pos - 1;
        }
        if (pos < cellIds.size() && cellIds.get(pos).rangeMin().lessOrEquals(id)) {
            return true;
        }
        return pos != 0 && cellIds.get(pos - 1).rangeMax().greaterOrEquals(id);
    }

    /**
     * Return true if the cell union intersects the given cell id. This is a fast
     * operation (logarithmic in the size of the cell union).
     */
    public boolean intersects(CellId id) {
        // This function requires that Normalize has been called first.
        // This is an exact test; see the comments for Contains() above.
        int pos = Collections.binarySearch(cellIds, id);

        if (pos < 0) {
            pos = -pos - 1;
        }


        if (pos < cellIds.size() && cellIds.get(pos).rangeMin().lessOrEquals(id.rangeMax())) {
            return true;
        }
        return pos != 0 && cellIds.get(pos - 1).rangeMax().greaterOrEquals(id.rangeMin());
    }

    public boolean contains(CellUnion that) {
        // TODO: A divide-and-conquer or alternating-skip-search approach
        // may be significantly faster in both the average and worst case.
        for (CellId id : that) {
            if (!this.contains(id)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Return true if this cell union contain/intersects the given other cell
     * union.
     */
    public boolean intersects(CellUnion union) {
        // TODO: A divide-and-conquer or alternating-skip-search approach
        // may be significantly faster in both the average and worst case.
        for (CellId id : union) {
            if (this.intersects(id)) {
                return true;
            }
        }
        return false;
    }

    public void getUnion(CellUnion x, CellUnion y) {
        cellIds.clear();
        cellIds.ensureCapacity(x.size() + y.size());
        cellIds.addAll(x.cellIds);
        cellIds.addAll(y.cellIds);
        normalize();
    }

    /**
     * Specialized version of GetIntersection() that gets the intersection of a
     * cell union with the given cell id. This can be useful for "splitting" a
     * cell union into chunks.
     */
    public void getIntersection(CellUnion x, CellId id) {
        // assert (x != this);
        cellIds.clear();
        if (x.contains(id)) {
            cellIds.add(id);
        } else {
            int pos = Collections.binarySearch(x.cellIds, id.rangeMin());

            if (pos < 0) {
                pos = -pos - 1;
            }

            CellId idmax = id.rangeMax();
            int size = x.cellIds.size();
            while (pos < size && x.cellIds.get(pos).lessOrEquals(idmax)) {
                cellIds.add(x.cellIds.get(pos++));
            }
        }
    }

    /**
     * Initialize this cell union to the union or intersection of the two given
     * cell unions. Requires: x != this and y != this.
     */
    public void getIntersection(CellUnion x, CellUnion y) {
        // This is a fairly efficient calculation that uses binary search to skip
        // over sections of both input vectors. It takes constant time if all the
        // cells of "x" come before or after all the cells of "y" in S2CellId order.

        cellIds.clear();

        int i = 0;
        int j = 0;

        while (i < x.cellIds.size() && j < y.cellIds.size()) {
            CellId imin = x.cellId(i).rangeMin();
            CellId jmin = y.cellId(j).rangeMin();
            if (imin.greaterThan(jmin)) {
                // Either j->contains(*i) or the two cells are disjoint.
                if (x.cellId(i).lessOrEquals(y.cellId(j).rangeMax())) {
                    cellIds.add(x.cellId(i++));
                } else {
                    // Advance "j" to the first cell possibly contained by *i.
                    j = indexedBinarySearch(y.cellIds, imin, j + 1);
                    // The previous cell *(j-1) may now contain *i.
                    if (x.cellId(i).lessOrEquals(y.cellId(j - 1).rangeMax())) {
                        --j;
                    }
                }
            } else if (jmin.greaterThan(imin)) {
                // Identical to the code above with "i" and "j" reversed.
                if (y.cellId(j).lessOrEquals(x.cellId(i).rangeMax())) {
                    cellIds.add(y.cellId(j++));
                } else {
                    i = indexedBinarySearch(x.cellIds, jmin, i + 1);
                    if (y.cellId(j).lessOrEquals(x.cellId(i - 1).rangeMax())) {
                        --i;
                    }
                }
            } else {
                // "i" and "j" have the same range_min(), so one contains the other.
                if (x.cellId(i).lessThan(y.cellId(j))) {
                    cellIds.add(x.cellId(i++));
                } else {
                    cellIds.add(y.cellId(j++));
                }
            }
        }
        // The output is generated in sorted order, and there should not be any
        // cells that can be merged (provided that both inputs were normalized).
    }

    /**
     * Just as normal binary search, except that it allows specifying the starting
     * value for the lower bound.
     *
     * @return The position of the searched element in the list (if found), or the
     * position where the element could be inserted without violating the
     * order.
     */
    private int indexedBinarySearch(List<CellId> l, CellId key, int low) {
        int high = l.size() - 1;

        while (low <= high) {
            int mid = (low + high) >> 1;
            CellId midVal = l.get(mid);
            int cmp = midVal.compareTo(key);

            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return mid; // key found
            }
        }
        return low; // key not found
    }

    /**
     * Expands the cell union such that it contains all cells of the given level
     * that are adjacent to any cell of the original union. Two cells are defined
     * as adjacent if their boundaries have any points in common, i.e. most cells
     * have 8 adjacent cells (not counting the cell itself).
     * <p>
     * Note that the size of the output is exponential in "level". For example,
     * if level == 20 and the input has a cell at level 10, there will be on the
     * order of 4000 adjacent cells in the output. For most applications the
     * Expand(min_fraction, min_distance) method below is easier to use.
     */
    public void expand(int level) {
        ArrayList<CellId> output = new ArrayList<>();
        long levelLsb = CellId.lowestOnBitForLevel(level);
        int i = size() - 1;
        do {
            CellId id = cellId(i);
            if (id.lowestOnBit() < levelLsb) {
                id = id.parent(level);
                // Optimization: skip over any cells contained by this one. This is
                // especially important when very small regions are being expanded.
                while (i > 0 && id.contains(cellId(i - 1))) {
                    --i;
                }
            }
            output.add(id);
            id.getAllNeighbors(level, output);
        } while (--i >= 0);
        initSwap(output);
    }

    public boolean mayIntersect(Cell cell) {
        return this.intersects(cell.id());
    }

    /**
     * The point 'p' does not need to be normalized. This is a fast operation
     * (logarithmic in the size of the cell union).
     */
    public boolean contains(Point p) {
        return contains(CellId.fromPoint(p));
    }

    /**
     * The number of leaf cells covered by the union.
     * This will be no more than 6*2^60 for the whole sphere.
     *
     * @return the number of leaf cells covered by the union
     */
    public long leafCellsCovered() {
        long numLeaves = 0;
        for (CellId cellId : cellIds) {
            int invertedLevel = CellId.MAX_LEVEL - cellId.level();
            numLeaves += (1L << (invertedLevel << 1));
        }
        return numLeaves;
    }

    /**
     * Normalizes the cell union by discarding cells that are contained by other
     * cells, replacing groups of 4 child cells by their parent cell whenever
     * possible, and sorting all the cell ids in increasing order. Returns true if
     * the number of cells was reduced.
     * <p>
     * This method *must* be called before doing any calculations on the cell
     * union, such as Intersects() or Contains().
     *
     * @return true if the normalize operation had any effect on the cell union,
     * false if the union was already normalized
     */
    public boolean normalize() {
        // Optimize the representation by looking for cases where all subcells
        // of a parent cell are present.

        ArrayList<CellId> output = new ArrayList<>(cellIds.size());
        output.ensureCapacity(cellIds.size());
        Collections.sort(cellIds);

        for (CellId id : this) {
            int size = output.size();
            // Check whether this cell is contained by the previous cell.
            if (!output.isEmpty() && output.get(size - 1).contains(id)) {
                continue;
            }

            // Discard any previous cells contained by this cell.
            while (!output.isEmpty() && id.contains(output.get(output.size() - 1))) {
                output.remove(output.size() - 1);
            }

            // Check whether the last 3 elements of "output" plus "id" can be
            // collapsed into a single parent cell.
            while (output.size() >= 3) {
                size = output.size();
                // A necessary (but not sufficient) condition is that the XOR of the
                // four cells must be zero. This is also very fast to test.
                if ((output.get(size - 3).id() ^ output.get(size - 2).id() ^ output.get(size - 1).id())
                        != id.id()) {
                    break;
                }

                // Now we do a slightly more expensive but exact test. First, compute a
                // mask that blocks out the two bits that encode the child position of
                // "id" with respect to its parent, then check that the other three
                // children all agree with "mask.
                long mask = id.lowestOnBit() << 1;
                mask = ~(mask + (mask << 1));
                long idMasked = (id.id() & mask);
                if ((output.get(size - 3).id() & mask) != idMasked
                        || (output.get(size - 2).id() & mask) != idMasked
                        || (output.get(size - 1).id() & mask) != idMasked || id.isFace()) {
                    break;
                }

                // Replace four children by their parent cell.
                output.remove(size - 1);
                output.remove(size - 2);
                output.remove(size - 3);
                id = id.parent();
            }
            output.add(id);
        }
        if (output.size() < size()) {
            initRawSwap(output);
            return true;
        }
        return false;
    }

    @Override
    public CellUnion clone() {
        CellUnion copy = new CellUnion();
        copy.initRawCellIds(Lists.newArrayList(cellIds));
        return copy;
    }

    /**
     * Return true if two cell unions are identical.
     */
    @Override
    public boolean equals(Object that) {
        if (!(that instanceof CellUnion)) {
            return false;
        }
        CellUnion union = (CellUnion) that;
        return this.cellIds.equals(union.cellIds);
    }

    @Override
    public int hashCode() {
        int value = 17;
        for (CellId id : this) {
            value = 37 * value + id.hashCode();
        }
        return value;
    }

}
