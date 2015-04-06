package org.apache.cassandra.gms2.membership;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.Sets;

/**
 * Implementation of the <a href="http://haslab.uminho.pt/cbm/files/1210.3368v1.pdf">
 * Observed Remove Set WithOut Tombstones</a>, a/k/a ORSWOT, paper. In short,
 * ORSWOT is a CRDT that uses a set as it's backing structure. The objective is to make a set
 * that is safe under eventual consistency and current updates - concurrent adds and removes of differing elements
 * on different nodes resolve nicely using logical clocks. However, the problem is with adding an element on one node,
 * and removing that element on another. Which wins? the add? the remove?
 *
 * The original OR-Set (section 3.3.5 of the CRDT paper) proposes using a second set to maintain the tombstoned elements.
 * ORSWOT proposes to ditch the tombstones and establishes rules about who wins on merge, using the logical clock
 * as an arbiter. You can have an 'add wins' or 'remove wins' set; we opt for add wins here. When an element is added to
 * the set, the logical clock is incremented; when the same element is removed, the clock remains unchanged. Thus,
 * when two nodes perform conflicting add/remove operations, the one that performed the add increment the clock,
 * and thus will be the 'winner' when the replicas converge.
 */
public class Orswot<T>
{
    private final InetAddress localAddr;
    private final AtomicReference<SetAndClock<T>> wrapper;

    public Orswot(InetAddress localAddr)
    {
        this.localAddr = localAddr;
        wrapper = new AtomicReference<>(new SetAndClock<>(new OrswotClock(localAddr), new HashSet<TaggedElement<T>>()));
    }

    /**
     * Add an element to the set. If the element is already in the set, the clock will be incremented,
     * as per the rules of 'add-wins' ORSWOT.
     * Called when this node is the coordinator for the update, not a downstream recipient.
     *
     * @param element The element to add to the set.
     */
    public void add(T element)
    {
        // perform an atomic swap of the wrapper (which contains the clock and the set)
        SetAndClock orig, next;
        do
        {
            orig = wrapper.get();
            HashSet<TaggedElement<T>> nextSet = orig.elements;
            OrswotClock nextClock = orig.clock.incrementCounter(localAddr);
            if (!nextSet.contains(element))
            {
                nextSet = new HashSet<>(nextSet);
                nextSet.add(new TaggedElement(element, nextClock.getClockValue(localAddr)));
            }
            next = new SetAndClock(nextClock, nextSet);
        } while (!wrapper.compareAndSet(orig, next));
    }

    /**
     * Remove an element from the set. If the element is already in the set, the clock will *not* be incremented,
     * as per the rule of 'add-wins' ORSWOT.
     * Called when this node is the coordinator for the update, not a downstream recipient.
     *
     * @param element The element to remove from the set.
     * @return <tt>true</tt> if this set contained the specified element.
     */
    public boolean remove(T element)
    {
        // perform an atomic swap of the wrapper (which contains the clock and the set)
        SetAndClock orig, next;
        do
        {
            orig = wrapper.get();
            TaggedElement<T> taggedElement = findElement(element, orig.elements);
            if (taggedElement == null)
                return false;

            HashSet<TaggedElement<T>> nextSet = orig.elements;
            nextSet = new HashSet<>(nextSet);
            nextSet.remove(element);
            next = new SetAndClock(orig.clock, nextSet);
        } while (!wrapper.compareAndSet(orig, next));

        return true;
    }

    /**
     * Find a element in a set (a function not provided by the JDK).
     *
     * @param t The element to find
     * @param set The set to search
     * @return first matching element, if any.
     */
    private TaggedElement<T> findElement(T t, Set<TaggedElement<T>> set)
    {
        for (TaggedElement<T> element : set)
        {
            if (t.equals(element.t))
                return element;
        }
        return null;
    }

    public Orswot<T> merge(Orswot<T> orswot)
    {
        SetAndClock<T> localWrapper = wrapper.get();
        Iterable<TaggedElement<T>> intersection = Sets.intersection(localWrapper.elements, orswot.getElements());




        return null;
    }

    /**
     * test that this set is in the other's semilattice
     */
    public boolean compare(Orswot<T> orswot)
    {
        //TODO: implement me
        return false;
    }

    public Set<TaggedElement<T>> getElements()
    {
        return wrapper.get().elements;
    }

    private static class SetAndClock<T>
    {
        private final HashSet<TaggedElement<T>> elements;
        private final OrswotClock clock;

        private SetAndClock(OrswotClock clock, HashSet<TaggedElement<T>> elements)
        {
            this.elements = elements;
            this.clock = clock;
        }
    }

    private static class TaggedElement<T>
    {
        private final T t;
        private final int clock;

        private TaggedElement(T t, int clock)
        {
            this.t = t;
            this.clock = clock;
        }

        public int hashCode()
        {
            return t.hashCode();
        }

        public boolean equals(Object o)
        {
            if (o == null || !(o instanceof TaggedElement))
                return false;
            return o == this || t.equals(((TaggedElement)o).t);
        }
    }
}
