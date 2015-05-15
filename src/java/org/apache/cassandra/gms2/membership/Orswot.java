package org.apache.cassandra.gms2.membership;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
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
 *
 * @param <T> The type to store in the ORSWOT. <b>MUST</b> implement proper Object#equals() and Object#hashCode() methods.
 * @param <A> The type of the actors of the ORSWOT
 */
public class Orswot<T, A>
{
    /**
     * Address of local machine. This is required when this node makes changes to the Orswot that will then be
     * communicated to peers.
     */
    private final A localAddr;
    private final AtomicReference<SetAndClock<T, A>> wrapper;

    public Orswot(A localAddr)
    {
        this.localAddr = localAddr;
        wrapper = new AtomicReference<>(new SetAndClock<T, A>(new OrswotClock<>(), new HashSet<TaggedElement<T, A>>()));
    }

    /**
     * Add an element to the set. If the element is already in the set, the clock will be incremented,
     * as per the rules of 'add-wins' ORSWOT.
     * Called when this node is the coordinator for the update, not a downstream recipient.
     *
     * @param t The element to add.
     */
    public void add(T t)
    {
        add(t, localAddr);
    }

    @VisibleForTesting
    void add(T t, A a)
    {
        // perform an atomic swap of the wrapper (which contains the clock and the set)
        SetAndClock<T, A> current, next;
        do
        {
            current = wrapper.get();
            OrswotClock<A> nextClock = current.clock.increment(a);
            HashSet<TaggedElement<T, A>> nextSet = new HashSet<>(current.elements);

            // this is a little funky, as HashSet.add() does *not* remove an existing entry.
            // so we have to remove it first. Wondering if SetAndClock should just use a map instead of a set...
            TaggedElement<T, A> taggedElement = new TaggedElement<>(t, nextClock);
            nextSet.remove(taggedElement);
            nextSet.add(taggedElement);

            next = new SetAndClock<>(nextClock, nextSet);
        } while (!wrapper.compareAndSet(current, next));
    }

    /**
     * Receive an update to the orswot that occurred on another node.
     */
    public void update()
    {
        // need updated master clock, affected node, node's new dot/tag (? - should it be the same as updated master?)
        // node addr that did updating
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
            TaggedElement<T, A> taggedElement = findElement(element, orig.elements);
            if (taggedElement == null)
                return false;

            HashSet<TaggedElement<T, A>> nextSet = orig.elements;
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
    private TaggedElement<T, A> findElement(T t, Set<TaggedElement<T, A>> set)
    {
        for (TaggedElement<T, A> element : set)
        {
            if (t.equals(element.t))
                return element;
        }
        return null;
    }

    public Orswot<T, A> merge(Orswot<T, A> orswot)
    {
        SetAndClock<T, A> localWrapper = wrapper.get();
        Iterable<TaggedElement<T, A>> intersection = Sets.intersection(localWrapper.elements, orswot.getElements());




        return null;
    }

    /**
     * test that this set is in the other's semilattice
     */
    public boolean compare(Orswot<T, A> orswot)
    {
        //TODO: implement me
        return false;
    }

    public Set<TaggedElement<T, A>> getElements()
    {
        return wrapper.get().elements;
    }

    public String toString()
    {
        return wrapper.get().toString();
    }

    @VisibleForTesting
    SetAndClock<T, A> getCurrentState()
    {
        return wrapper.get();
    }

    static class SetAndClock<T, A>
    {
        final HashSet<TaggedElement<T, A>> elements;
        final OrswotClock<A> clock;

        private SetAndClock(OrswotClock<A> clock, HashSet<TaggedElement<T, A>> elements)
        {
            this.elements = elements;
            this.clock = clock;
        }

        public String toString()
        {
            return "Orswort state: master clock = " + clock.toString() +
                   ", tagged entries = " + elements.toString();
        }
    }

    /**
     * Capture the 'dot' associated with each specific element in the ORSWOT when it is added.
     * Note that elements in the ORSWOT are different than the actors who participate in modifying the ORSWOT.
     * @param <T>
     */
    static class TaggedElement<T, A>
    {
        final T t;
        final OrswotClock<A> clock;

        private TaggedElement(T t, OrswotClock<A> clock)
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

        public String toString()
        {
            return t.toString() + " : " + clock.toString();
        }
    }
}
