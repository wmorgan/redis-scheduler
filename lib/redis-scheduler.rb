## A simple, production-ready chronological scheduler for Redis.
##
## Use #schedule! to add an item to be processed at an arbitrary point in time.
## The item will be converted to a string and returned to you as such.
##
## Use #each to iterate over those items at the scheduled time. This call
## iterates over all items that are scheduled on or before the current time, in
## chronological order. In blocking mode, this call will wait forever until
## such items become available, and will never terminate. In non-blocking mode,
## this call will only iterate over ready items and will terminate when there
## are no items ready for processing.
##
## Use #items to simply iterate over all items in the queue, for debugging
## purposes.
##
## == Exceptions during processing
##
## Any exceptions during #each will result in the item being re-added to the
## schedule at the original time.
##
## == Multiple producers and consumers
##
## Multiple producers and consumers are fine.
##
## == Concurrent reads and writes
##
## Concurrent reads and writes are fine.
##
## == Segfaults
##
## The scheduler maintains a "processing set" of items currently being
## processed. If a process dies (i.e. not as a result of a Ruby exception, but
## as the result of a segfault), the item will remain in this set but will
## not longer appear in the schedule. To avoid losing scheduled work due to
## segfaults, you must periodically iterate through this set and recover
## any items that have been abandoned, using #processing_set_items. Setting a
## proper 'descriptor' argument in #each is suggested.
class RedisScheduler
  include Enumerable

  POLL_DELAY = 1.0 # seconds
  CAS_DELAY  = 0.5 # seconds

  ## Options:
  ## * +namespace+: prefix for Redis keys, e.g. "scheduler/".
  ## * +blocking+: whether #each should block or return immediately if there are items to be processed immediately.
  ## * +uniq+: when false (default), the same item can be scheduled for multiple times. When true, scheduling the same item multiple times only updates its scheduled time, and does not represent the item multiple times in the schedule.
  ##
  ## Note that uniq is set on a per-schedule basis and cannot be changed. Once
  ## a uniq schedule is created, it is forever uniq (until #reset! is called,
  ## at least). Attempts to use non-uniq queues in a uniq manner, or vice versa,
  ## will result in undefined behavior (probably errors).
  ##
  ## Note also that nonblocking mode may still actually block momentarily as
  ## part of the check-and-set semantics, i.e. block during contention from
  ## multiple clients. "Nonblocking" refers to whether the scheduler should
  ## wait until events in the schedule are ready, or only return those items
  ## that are ready currently.
  def initialize redis, opts={}
    @redis = redis
    @namespace = opts[:namespace]
    @blocking = opts[:blocking]
    @uniq = opts[:uniq]

    @queue = [@namespace, "q"].join
    @processing_set = [@namespace, "processing"].join
    @counter = [@namespace, "counter"].join
  end

  ## Schedule an item at a specific time. item will be converted to a string.
  def schedule! item, time
    @redis.zadd @queue, time.to_f, make_entry(item)
  end

  ## Drop all data and reset the schedule entirely.
  def reset!
    [@queue, @processing_set, @counter].each { |k| @redis.del k }
  end

  ## Return the total number of items in the schedule.
  def size; @redis.zcard @queue end

  ## Returns the total number of items currently being processed.
  def processing_set_size; @redis.scard @processing_set end

  ## Yields items along with their scheduled times. Only returns items on or
  ## after their scheduled times. Items are returned as strings. If @blocking is
  ## false, will stop once there are no more items that can be processed
  ## immediately; if it's true, will wait until items become available (and
  ## never terminate).
  ##
  ## +Descriptor+ is an optional string that will be associated with this item
  ## while in the processing set. This is useful for providing whatever
  ## information you need to determine whether the item needs to be recovered
  ## when iterating through the processing set.
  def each descriptor=nil
    while(x = get(descriptor))
      item, processing_descriptor, at = x
      begin
        yield item, at
      rescue Exception # back in the hole!
        schedule! item, at
        raise
      ensure
        cleanup! processing_descriptor
      end
    end
  end

  ## Returns an Enumerable of [item, scheduled time] pairs, which can be used
  ## to iterate over all the items in the queue, in order of earliest- to
  ## latest-scheduled, regardless of the schedule time.
  ##
  ## Note that this view is not synchronized with write operations, and thus
  ## may be inconsistent (e.g. return duplicates, miss items, etc) if changes
  ## to the schedule happen while iterating.
  ##
  ## For these reasons, this is mainly useful for debugging purposes.
  def items; ItemEnumerator.new(@redis, @queue, @uniq) end

  ## Returns an Array of [item, timestamp, descriptor] tuples representing the
  ## set of in-process items. The timestamp corresponds to the time at which
  ## the item was removed from the schedule for processing.
  def processing_set_items
    @redis.smembers(@processing_set).map do |x|
      item, timestamp, descriptor = Marshal.load(x)
      [item, Time.at(timestamp), descriptor]
    end
  end

private

  ## generate the value actually stored in redis
  def make_entry item
    if @uniq
      item.to_s
    else
      id = @redis.incr @counter
      "#{id}:#{item}"
    end
  end

  ## the inverse of #make_item_value
  def parse_entry entry
    if @uniq
      entry
    else
      entry =~ /^\d+:(\S+)$/ or raise InvalidEntryException, entry
      $1
    end
  end

  def get descriptor; @blocking ? blocking_get(descriptor) : nonblocking_get(descriptor) end

  def blocking_get descriptor
    sleep POLL_DELAY until(x = nonblocking_get(descriptor))
    x
  end

  ## Thrown by some RedisScheduler operations if the item in Redis zset
  ## underlying the schedule is not parseable. This should basically never
  ## happen, unless you are naughty and are adding/removing items from that
  ## zset yourself.
  class InvalidEntryException < StandardError; end
  def nonblocking_get descriptor
    loop do
      @redis.watch @queue
      entries = @redis.zrangebyscore @queue, 0, Time.now.to_f, :withscores => true, :limit => [0, 1]
      break unless entries.size > 0
      entry, at = entries.first
      item = parse_entry entry
      descriptor = Marshal.dump [item, Time.now.to_i, descriptor]
      @redis.multi do # try and grab it
        @redis.zrem @queue, entry
        @redis.sadd @processing_set, descriptor
      end and break [item, descriptor, Time.at(at.to_f)]
      sleep CAS_DELAY # transaction failed. retry!
    end
  end

  def cleanup! item
    @redis.srem @processing_set, item
  end

  ## Enumerable class for iterating over everything in the schedule. Paginates
  ## calls to Redis under the hood (and is thus usable for very large
  ## schedules), but is not synchronized with write traffic and thus may return
  ## duplicates or skip items when paginating.
  ##
  ## Supports random access with #[], with the same caveats as above.
  class ItemEnumerator
    include Enumerable
    def initialize redis, q, uniq
      @redis = redis
      @q = q
      @uniq = uniq
    end

    PAGE_SIZE = 50
    def each
      start = 0
      while start < size
        elements = self[start, PAGE_SIZE]
        elements.each { |*x| yield(*x) }
        start += elements.size
      end
    end

    def [] start, num=nil
      elements = @redis.zrange @q, start, start + (num || 0), :withscores => true
      v = elements.map do |entry, at|
        item = parse_entry entry
        [item, Time.at(at.to_f)]
      end
      num ? v : v.first
    end

    def size; @redis.zcard @q end

    ## duplicated :(
    def parse_entry entry
      if @uniq
        entry
      else
        entry =~ /^\d+:(\S+)$/ or raise InvalidEntryException, entry
        $1
      end
    end
  end
end
