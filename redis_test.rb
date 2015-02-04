require "redis"
require "json"
require "pp"
require "zlib"
require "csv"

redis = Redis.new(db: 5)

tests = [
    [1000000, 100]
  ]

# Write to CSV
CSV.open("./redis_tests.csv", "wb") do |csv|

  for t in tests do
    aud_size = t[0].to_i || 1000000
    max_ints = t[1].to_i || 30

    # Traditional Key/Value
    redis.flushdb
    strategy = "Key/Value"

    aud_size.times do |i|
      user_id = Random.rand(1000..999999999)

      Random.rand(max_ints).times do
        interest = Random.rand(500)
        score = Random.rand()
        key = user_id.to_s + ":" + interest.to_s
        redis.set(key, score)
      end

      if i>0 && i%10000 == 0
        mem_used = redis.info("memory")["used_memory"]
        csv << [i, max_ints, strategy, mem_used]
        csv.flush
      end

    end


    # Bitmap where bit signals interest_id
    redis.flushdb
    strategy = "Bitmap"

    aud_size.times do |i|
      user_id = Random.rand(1000..999999999)

      Random.rand(max_ints).times do
        interest = Random.rand(500)

        redis.setbit(user_id, interest, 1)
      end

      if i>0 && i%10000 == 0
        mem_used = redis.info("memory")["user_memory"]
        csv << [i, max_ints, strategy, mem_used]
        csv.flush
      end

    end

    # User is key, set of scores where index is interest ID
    # Note: Memory usage on this explodes
    # puts "User ID is key, scores stored in array with interest as index"
    #redis.flushdb

    #aud_size.times do |i|
      #user_id = Random.rand(1000..999999999)
      #500.times do
        #redis.lpush(user_id, "")
      #end

      #Random.rand(max_ints).times do
        #interest = Random.rand(500)
        #score = Random.rand().round(2)
        #redis.lset(user_id, interest, score)
      #end
    #end
    #puts "Used Memory: " + redis.info("memory")["user_memory"]


    # User is key, scores stored in hash with interest as key
    redis.flushdb
    strategy = "Hash"

    aud_size.times do |i|
      user_id = Random.rand(1000..999999999)

      Random.rand(max_ints).times do
        interest = Random.rand(500)
        score = Random.rand(0..99)
        redis.hset(user_id, interest, score)
      end


      if i>0 && i%10000 == 0
        mem_used = redis.info("memory")["user_memory"]
        csv << [i, max_ints, strategy, mem_used]
        csv.flush
      end

    end

    # MessagePack encode scores hash
    redis.flushdb
    strategy = "Msgpack Hash as Value"

    # Add message pack encoder script
    msgpack_encode_script = "local key = KEYS[1];
    local value = ARGV[1];
    local mvalue = cmsgpack.pack(value);
    return redis.call('SET', key, mvalue);"
    sha = redis.script(:load, msgpack_encode_script)

    aud_size.times do |i|
      user_id = Random.rand(1000..999999999)

      interestHash = {}

      Random.rand(max_ints).times do
        interest = Random.rand(500)
        score = Random.rand(0..99)

        interestHash[interest] = score
      end

      redis.evalsha(sha, [user_id], [interestHash.to_s])

      if i>0 && i%10000 == 0
        mem_used = redis.info("memory")["user_memory"]
        csv << [i, max_ints, strategy, mem_used]
        csv.flush
      end

    end

    # Compress interest map
    redis.flushdb
    strategy = "Compress hash with zlib"

    aud_size.times do |i|
      user_id = Random.rand(1000..999999999)

      interestHash = {}

      Random.rand(max_ints).times do
        interest = Random.rand(500)
        score = Random.rand(0..99)

        interestHash[interest] = score
      end
      redis.set(user_id, Zlib::Deflate.deflate(interestHash.to_s))

      if i>0 && i%10000 == 0
        mem_used = redis.info("memory")["user_memory"]
        csv << [i, max_ints, strategy, mem_used]
        csv.flush
      end

    end

  end

end
