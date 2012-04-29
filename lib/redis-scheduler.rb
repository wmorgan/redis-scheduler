class RedisScheduler
  include Enumerable

  POLL_DELAY = 1.0 # seconds
  CAS_DELAY  = 0.5 # seconds

  ## options:
  ##   namespace: prefix for redis data, e.g. "scheduler/"
  ##   blocking: whether #each should block or return immediately if
  ##     there are items to be processed immediately.
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
    @error_queue = [@namespace, "errorq"].join
    @counter = [@namespace, "counter"].join
  end

  ## schedule an item at a specific time. item will be converted to a
  ## string.
  def schedule! item, time
    id = @redis.incr @counter
    @redis.zadd @queue, time.to_f, "#{id}:#{item}"
  end

  def reset!
    [@queue, @error_queue, @counter].each { |k| @redis.del k }
  end

  def size; @redis.zcard @queue end
  def error_queue_size; @redis.llen @error_queue end

  ## yields items along with their scheduled times. only returns items
  ## on or after their scheduled times. items returned as strings. if
  ## @blocking is false, will stop once there are no more items that can
  ## be processed immediately; if it's true, will wait until items
  ## become available (and never terminate).
  def each
    while(x = get)
      item, erritem, at = x
      begin
        yield item, at
      rescue Exception # back in the hole!
        schedule! item, at
        raise
      ensure
        cleanup! erritem
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

private

  def get; @blocking ? blocking_get : nonblocking_get end

  def blocking_get
    sleep POLL_DELAY until(x = nonblocking_get)
    x
  end

  class InvalidEntryException < StandardError; end
  def nonblocking_get
    catch :cas_retry do
      @redis.watch @queue
      item, at = @redis.zrangebyscore @queue, 0, Time.now.to_f,
        :withscores => true, :limit => [0, 1]
      if item
        @redis.multi do # try and grab it
          @redis.zrem @queue, item
          @redis.lpush @error_queue, item
        end or begin
          sleep CAS_DELAY
          throw :cas_retry
        end
        item =~ /^\d+:(\S+)$/ or raise InvalidEntryException, item
        original = $1
        [original, item, Time.at(at.to_f)]
      end
    end
  end

  def cleanup! item
    @redis.lrem @error_queue, 1, item
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
