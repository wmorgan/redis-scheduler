require 'minitest/autorun'
require 'redis'
require 'redis-scheduler'
require 'timecop'

## WARNING! This currently writes a bunch of keys to the Redis running on the
## default location. The keys are prefixed with
## "test-8ee2027983915ec78acc45027d874316/" and
## "test-8ee2027983915ec78acc45027d874317/" to avoid colliding with anything
## important, but this is not a great strategy, eh?

class RedisSchedulerTest < Minitest::Test
  TIME = Time.local 2012, 1, 1, 15, 0, 0

  def setup
    super
    @redis = Redis.new
    @scheduler = RedisScheduler.new @redis, namespace: "test-8ee2027983915ec78acc45027d874316/", blocking: false
    @uniq_scheduler  = RedisScheduler.new @redis, namespace: "test-8ee2027983915ec78acc45027d874317/", blocking: false, uniq: true
  end

  def teardown
    super
    @scheduler.reset!
    @uniq_scheduler.reset!
  end

  def test_begins_empty
    assert_equal 0, @scheduler.size
  end

  def test_stuff_waits
    Timecop.freeze TIME do
      @scheduler.schedule! "a", (TIME + 100)
      got_item = false
      @scheduler.each { got_item = true }
      assert !got_item
    end

    Timecop.freeze(TIME + 50) do
      got_item = false
      @scheduler.each { got_item = true }
      assert !got_item
    end

    Timecop.freeze(TIME + 100) do
      got_item = false
      @scheduler.each { got_item = true }
      assert got_item
    end
  end

  def test_can_get_items_later_than_their_timestamp
    Timecop.freeze TIME do
      @scheduler.schedule! "a", (TIME + 100)
    end

    Timecop.freeze(TIME + 200) do
      got_item = false
      @scheduler.each { got_item = true }
      assert got_item
    end
  end

  def test_ordering_is_based_on_timestamp
    Timecop.freeze TIME do
      @scheduler.schedule! "b", (TIME + 101)
      @scheduler.schedule! "a", (TIME + 100)

      el, ts = @scheduler.items[0]
      assert_equal "a", el
      assert_equal TIME + 100, ts

      el, ts = @scheduler.items[1]
      assert_equal "b", el
      assert_equal TIME + 101, ts

      assert_equal [["a", TIME + 100], ["b", TIME + 101]], @scheduler.items[0, 2]
    end
  end

  def test_can_schedule_same_item_multiple_times
    Timecop.freeze TIME do
      @scheduler.schedule! "a", (TIME + 101)
      @scheduler.schedule! "a", (TIME + 100)

      assert_equal [["a", TIME + 100], ["a", TIME + 101]], @scheduler.items[0, 2]
    end
  end

  def test_uniq_schedules_update_items_rather_than_representing_them_multiple_times
    Timecop.freeze TIME do
      @uniq_scheduler.schedule! "a", (TIME + 101)
      @uniq_scheduler.schedule! "a", (TIME + 100)

      assert_equal [["a", TIME + 100]], @uniq_scheduler.items[0, 2]
    end
  end

  def test_oldest_always_wins
    Timecop.freeze TIME do
      @scheduler.schedule! "a", (TIME + 101)
    end

    Timecop.freeze(TIME + 50) do
      @scheduler.schedule! "b", (TIME + 100)
    end

    Timecop.freeze(TIME + 200) do
      items = []
      @scheduler.each do |item, ts|
        items << item
      end

      assert_equal ["b", "a"], items
    end
  end
end
