class RedisScheduler
  include Enumerable

  POLL_DELAY = 1.0 # seconds
  CAS_DELAY  = 0.5 # seconds

  ## options:
  ## * +namespace+: prefix for redis data, e.g. "scheduler/"
  ## * +blocking+: whether #each should block or return immediately if there are items to be processed immediately.
  ##
  ## Note that nonblocking mode may still actually block as part of the
  ## check-and-set semantics, i.e. block during contention from multiple
  ## clients. "Nonblocking" mode just refers to whether the scheduler
  ## should wait until events in the schedule are ready, or only return
  ## those items that are ready currently.
  def initialize redis, opts={}
    @redis = redis
    @namespace = opts[:namespace]
    @blocking = opts[:blocking]

    @queue = [@namespace, "q"].join
    @processing_set = [@namespace, "processing"].join
    @counter = [@namespace, "counter"].join
  end

  ## schedule an item at a specific time. item will be converted to a
  ## string.
  def schedule! item, time
    id = @redis.incr @counter
    @redis.zadd @queue, time.to_f, "#{id}:#{item}"
  end

  def reset!
    [@queue, @processing_set, @counter].each { |k| @redis.del k }
  end

  def size; @redis.zcard @queue end

  ## Returns the total number of items currently being processed.
  def processing_set_size; @redis.scard @processing_set end

  ## Yields items along with their scheduled times. only returns items on or
  ## after their scheduled times. items are returned as strings. if @blocking is
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

  ## returns an Enumerable of [item, schedule time] pairs, which can be used to
  ## easily iterate over all the items in the queue, in order of earliest- to
  ## latest-scheduled. note that this view is not coordinated with write
  ## operations, and may be inconsistent (e.g. return duplicates, miss items,
  ## etc).
  ##
  ## for these reasons, this operation is mainly useful for debugging purposes.
  def items; ItemEnumerator.new(@redis, @queue) end

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

  def get descriptor; @blocking ? blocking_get(descriptor) : nonblocking_get(descriptor) end

  def blocking_get descriptor
    sleep POLL_DELAY until(x = nonblocking_get(descriptor))
    x
  end

  class InvalidEntryException < StandardError; end
  def nonblocking_get descriptor
    catch :cas_retry do
      @redis.watch @queue
      entry, at = @redis.zrangebyscore @queue, 0, Time.now.to_f, :withscores => true, :limit => [0, 1]
      if entry
        entry =~ /^\d+:(\S+)$/ or raise InvalidEntryException, entry
        item = $1
        processing_descriptor = Marshal.dump [item, Time.now.to_i, descriptor]
        @redis.multi do # try and grab it
          @redis.zrem @queue, entry
          @redis.sadd @processing_set, processing_descriptor
        end or begin
          sleep CAS_DELAY
          throw :cas_retry
        end
        [item, processing_descriptor, Time.at(at.to_f)]
      end
    end
  end

  def cleanup! item
    @redis.srem @processing_set, item
  end

  ## enumerable for just iterating over everything in the queue
  class ItemEnumerator
    include Enumerable
    def initialize redis, q
      @redis = redis
      @q = q
    end

    BLOCK_SIZE = 10
    def each
      start = 0
      while start < size
        elements = @redis.zrange @q, start, start + BLOCK_SIZE,
          :withscores => true
        elements.each_slice(2) do |item, at| # isgh
          item =~ /^\d+:(\S+)$/ or raise InvalidEntryException, item
          item = $1
          yield item, Time.at(at.to_f)
        end
        start += elements.size
      end
    end

    def size; @redis.zcard @q end
  end

end
