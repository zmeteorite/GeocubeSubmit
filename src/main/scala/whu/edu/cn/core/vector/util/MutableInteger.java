package whu.edu.cn.core.vector.util;

/**
 * Like an Integer, but mutable :)
 * <p>
 * Sometimes it is just really convenient to be able to pass a MutableInteger
 * as a parameter to a function, or for synchronization purposes (so that you
 * can guard access to an int value without creating a separate Object just to
 * synchronize on).
 * <p>
 * NOT thread-safe
 */
public class MutableInteger {

    private int value;
    private Integer cachedIntegerValue = null;

    public MutableInteger(final int i) {
        value = i;
    }

    public int intValue() {
        return value;
    }

    public Integer integerValue() {
        if (cachedIntegerValue == null) {
            cachedIntegerValue = intValue();
        }
        return cachedIntegerValue;
    }

    @Override
    public boolean equals(final Object o) {
        return o instanceof MutableInteger && ((MutableInteger) o).value == this.value;
    }

    @Override
    public int hashCode() {
        return integerValue().hashCode();
    }

    public void setValue(final int value) {
        this.value = value;
        cachedIntegerValue = null;
    }

    public void increment() {
        add(1);
    }

    public void add(final int amount) {
        setValue(value + amount);
    }

    public void decrement() {
        subtract(1);
    }

    public void subtract(final int amount) {
        add(amount * -1);
    }

    @Override
    public String toString() {
        return String.valueOf(value);
    }
}
