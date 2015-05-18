package org.apache.cassandra.gms2.membership;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;

/**
 * Implementation of the <a href="http://haslab.uminho.pt/cbm/files/1210.3368v1.pdf">
 * Observed Remove Set WithOut Tombstones</a>, a/k/a ORSWOT, paper. In short,
 * ORSWOT is a CRDT that uses a set as it's backing structure. The objective is to make a set
 * that is safe under eventual consistency and current updates - concurrent adds and removeTimestamps of differing elements
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

    /**
     * When we receive a remove operation with a clock timestamp for which we have not seen
     * a descending (greater than or equal to) add clock, we need to store remove request until
     * the corresponding add is received. Entries are only deleted when an add with a dominating
     * clock timestamp is received.
     */
    private final ConcurrentMap<T, OrswotClock<A>> removeTimestamps;

    public Orswot(A localAddr)
    {
        this.localAddr = localAddr;
        wrapper = new AtomicReference<>(new SetAndClock<T, A>(new OrswotClock<>(), new HashSet<TaggedElement<T, A>>()));
        removeTimestamps = new ConcurrentHashMap<>();
    }

    /**
     * Add an element to the set. If the element is already in the set, the clock will be incremented,
     * as per the rules of 'add-wins' ORSWOT.
     * Called when this node is the coordinator for the update, not a downstream recipient.
     *
     * @param t The element to add.
     */
    public OrswotClock<A> add(T t)
    {
        return add(t, localAddr);
    }

    @VisibleForTesting
    OrswotClock<A> add(T t, A a)
    {
        // perform an atomic swap of the wrapper (which contains the clock and the set)
        SetAndClock<T, A> current, next = null;
        do
        {
            current = wrapper.get();
            OrswotClock<A> nextClock = current.clock.increment(a);

            OrswotClock<A> removeTimestamp = removeTimestamps.get(t);
            if (removeTimestamp != null)
            {
                if (!nextClock.dominates(removeTimestamp))
                {
                    // it is possible another node performs an add which we do not receive, but we do receive a remove
                    // for the same element (which we stored in removeTimestamp). Then, this node is asked to add the node,
                    // but we have that pesky remove timestamp we have to deal with. Thus, we need
                    // to merge that remove timestamp and any new clock we create - that way downstream recipients
                    // have an easy job to reconciling and merging the new add.
                    nextClock = nextClock.merge(removeTimestamp);

                    if (!nextClock.dominates(removeTimestamp))
                    {
                        // i think we can only get here via a programming error; basically if nextClock.equals(removeTimestamp)
                        String msg = String.format("next clock does not dominate the existing remove timestamp; next: %s, remove: %s", nextClock, removeTimestamp);
                        throw new IllegalStateException(msg);
                    }
                }
                // if we could not remove the delete, it was because we lost a data race and there is
                // a clock with a dominating timestamp for the delete entry. just retry the loop for simplicity.
                if (!removeFromRemoveTimestamps(removeTimestamps, t, nextClock))
                    continue;
            }

            HashSet<TaggedElement<T, A>> nextSet = new HashSet<>(current.elements);

            // this is a little funky, as HashSet.add() does *not* remove an existing entry.
            // so we have to remove it first. Wondering if SetAndClock should just use a map instead of a set...
            TaggedElement<T, A> taggedElement = new TaggedElement<>(t, nextClock);
            nextSet.remove(taggedElement);
            nextSet.add(taggedElement);

            next = new SetAndClock<>(nextClock, nextSet);
        } while (next != null && !wrapper.compareAndSet(current, next));

        removeTimestamps.remove(t);
        return next.clock;
    }

    /**
     * Apply an add to the set.
     *
     * @param t Element to add
     * @param clock timestamp of addition
     * @return
     */
    public boolean applyAdd(T t, OrswotClock<A> clock)
    {
        // perform an atomic swap of the wrapper (which contains the clock and the set)
        SetAndClock<T, A> current, next = null;
        do {
            current = wrapper.get();
            OrswotClock<A> existingClock = getClock(t);

            // if we have an existing clock, check that the new one descends, else merge the two clock
            if (existingClock != null)
            {
                if (existingClock.descends(clock))
                    return false;

                // merge clocks
                clock = clock.merge(existingClock);
                if (!clock.dominates(existingClock))
                {
                    // i think we can only get here via a programming error; basically if clock.equals(existingClock)
                    String msg = String.format("merged clock does not dominate the existing clock; merged: %s, existing: %s", clock, existingClock);
                    throw new IllegalStateException(msg);
                }
            }

            // check the deferred removeTimestamps collection to see if there is an entry blocking this add
            OrswotClock<A> existingRemove = removeTimestamps.get(t);
            if (existingRemove != null)
            {
                if (!clock.dominates(existingRemove))
                    return false;

                // if new clock dominates, remove the timestamp from the deletes
                if (!removeFromRemoveTimestamps(removeTimestamps, t, clock))
                {
                    // if we could not remove the delete, it was because we lost the race and there is
                    // a clock with a dominating timestamp for the delete entry. just retry the loop for simplicity.
                    continue;
                }
            }

            TaggedElement<T, A> taggedElement = new TaggedElement<>(t, clock);

            // see comment in add() about the use of HashSet
            HashSet<TaggedElement<T, A>> nextSet = new HashSet<>(current.elements);
            nextSet.remove(taggedElement);
            nextSet.add(taggedElement);
            next = new SetAndClock<>(current.clock.merge(clock), nextSet);
        } while (next != null && !wrapper.compareAndSet(current, next));

        return true;
    }

    /**
     * Remove an element from the orswot. Element will be removed if 1) is actually exists in the orswort,
     * and 2) if the {@code clock} is equal to the element's existing clock.
     *
     * If the {@code clock} dominates the element's existing clock, it is assumed the remove arrived before
     * the add with the same clock timestamp, and thus we *cannot* remove the element. We will, however, store the
     * remove so that if we receive an add with the same timestamp, the element will not be added to the orswot.
     * If the add contains a clock that dominates the remove's clock, the entry in the deferred collection will be dropped
     * (and the element will be properly added to the orswot).
     *
     * @param t Element to remove
     * @param clock removal timestamp
     * @return true if the element was removed or was added to {@code removeTimestamps}.
     */
    public boolean applyRemove(T t, OrswotClock<A> clock)
    {
        SetAndClock<T, A> current, next;
        do {
            current = wrapper.get();
            OrswotClock<A> existingClock = getClock(t);

            // if there's no existing entry in the orswot, just put the remove clock in the removeTimestamps
            if (existingClock == null)
                return addToRemoveTimestamps(removeTimestamps, t, clock);

            if (clock.equals(existingClock))
            {
                /// this is the case want
                TaggedElement<T, A> taggedElement = new TaggedElement<>(t, clock);

                // see comment in add() about the use of HashSet
                HashSet<TaggedElement<T, A>> nextSet = new HashSet<>(current.elements);
                nextSet.remove(taggedElement);
                next = new SetAndClock<>(current.clock.merge(clock), nextSet);
            }
            else if (clock.dominates(existingClock))
            {
                return addToRemoveTimestamps(removeTimestamps, t, clock);
            }
            else
            {
                // we already received a newer add, so this is a dupe or delayed message
                return false;
            }
        } while (!wrapper.compareAndSet(current, next));

        return addToRemoveTimestamps(removeTimestamps, t, clock);
    }

    /**
     * Add element to the removeTimestamps collection.
     *
     * @return true if there is not a 'live' entry in orswot with a descending clock, or if there exists an entry
     * in {@code removeTimestamps} for the element, that clock is dominated by {@code clock}.
     */
    @VisibleForTesting
    boolean addToRemoveTimestamps(ConcurrentMap<T, OrswotClock<A>> removeTimestamps, T t, OrswotClock<A> clock)
    {
        while (true)
        {
            // check if we have an add operation that has a dominating clock
            OrswotClock<A> liveClock = getClock(t);
            if (liveClock != null && liveClock.dominates(clock))
                return false;

            OrswotClock<A> existingClock = removeTimestamps.putIfAbsent(t, clock);
            if (existingClock == null)
                return true;

            // check if we lost the race to another remove timestamp with a higher value
            if (!clock.dominates(existingClock))
                return false;

            // this is a CAS operation which, if we lost the thread race, will cause the loop to execute again
            if (removeTimestamps.replace(t, existingClock, clock))
                return true;
        }
    }

    @VisibleForTesting
    boolean removeFromRemoveTimestamps(ConcurrentMap<T, OrswotClock<A>> removeTimestamps, T t, OrswotClock<A> clock)
    {
        OrswotClock<A> current;
        do
        {
             current = removeTimestamps.get(t);
            if (current == null)
                return true;
            if (!clock.dominates(current))
                return false;
        } while (!removeTimestamps.remove(t, current));
        return true;
    }

    /**
     * Reteive the logical clock for the given parameter. If present the clock will reflect the last time
     * the element was added to the orswot. (Remember, on adds of existing elements, we update the clock to the current 'time').
     *
     * @param t The element to get the clock for.
     * @return Clock of the element if it is present in the orswot; else, null.
     */
    public OrswotClock<A> getClock(T t)
    {
        SetAndClock<T, A> current = wrapper.get();
        TaggedElement<T, A> taggedElement = findElement(t, current.elements);
        return taggedElement != null ? taggedElement.clock : null;
    }

    /**
     * Remove an element from the set.
     * Called when this node is the coordinator for the update, not a downstream recipient.
     * If the target is not a member of the Orswot, returns null. (NOTE: we can revisit this later)
     */
    public OrswotClock<A> remove(T t)
    {
        OrswotClock<A> elementClock;
        SetAndClock<T, A> current, next = null;
        do
        {
            current = wrapper.get();
            TaggedElement<T, A> taggedElement = findElement(t, current.elements);

            // if we don't know about the target, it could be that we never received an 'add', just a 'remove'
            elementClock = taggedElement != null ? taggedElement.clock : null;

            OrswotClock<A> existingRemove = removeTimestamps.get(t);
            if (elementClock != null)
            {
                if (existingRemove == null)
                {
                    // if we could not add the delete, it was because we lost the race and there is
                    // a clock with a dominating timestamp for the delete entry. just retry the loop for simplicity.
                    if (!addToRemoveTimestamps(removeTimestamps, t, elementClock))
                        continue;
                }
                else
                {
                    // so, we have both a live element and a remove timestamp - not sure this could happen outside of a data race
                    if (existingRemove.descends(elementClock))
                        return existingRemove;
                }
            }
            else if (existingRemove != null)
            {
                // no live element, but we received a remove request, so just use the existing remove timestamp (that was
                // received from another node).
                elementClock = existingRemove;
            }
            else
            {
                // we don't have a live element nor an existing remove timestamp, bail as we have no legit clock
                // to base the remove upon.
                return null;
            }

            // see comment in add() about the use of HashSet
            HashSet<TaggedElement<T, A>> nextSet = new HashSet<>(current.elements);
            nextSet.remove(taggedElement);
            next = new SetAndClock<>(current.clock, nextSet);
        } while (next != null && !wrapper.compareAndSet(current, next));

        return elementClock;
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

    @VisibleForTesting
    ConcurrentMap<T, OrswotClock<A>> getRemoveTimestamps()
    {
        return removeTimestamps;
    }

    /**
     * A wrapper class class that holds a state of the orswot at a given moment
     *
     * @param <T> The type to store in the ORSWOT.
     * @param <A> The type of the actors of the ORSWOT
     */
    static class SetAndClock<T, A>
    {
        /**
         * 'master' clock as of the last modification.
         */
        final OrswotClock<A> clock;

        /**
         * Collection of the actual elements stored within the orswot.
         */
        final HashSet<TaggedElement<T, A>> elements;

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
     *
     * @param <T> The type to store in the ORSWOT.
     */
    static class TaggedElement<T, A>
    {
        /**
         * The element that's actually being stored.
         */
        final T t;

        /**
         * A logical timestamp of when the element was added (or re-added) to the orswot.
         */
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
            return !(o == null || !(o instanceof TaggedElement)) && (o == this || t.equals(((TaggedElement) o).t));
        }

        public String toString()
        {
            return t.toString() + " : " + clock.toString();
        }
    }
}
