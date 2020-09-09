
               -- With lua float we are losing precision for this reason
               -- we keep the number as a string
               local is_sup = function(a,b)
                 local int_a = string.match(a,"%d+")
                 local int_b = string.match(b,"%d+")
                 if string.len(int_a) > string.len(int_b) then
                   return true;
                 end;
                 return a > b;
               end;
               
        -- Note that bucket is the key name, not just the bucket name
        -- (it has a prefix).
        local update_bucket_stats = function(
            container_key, bucket_key, buckets_list_key,
            account, container_name, bucket_name, mtime, deleted,
            inc_objects, inc_bytes, inc_damaged_objects, inc_missing_chunks)
          if deleted then
            redis.call('HDEL', container_key, 'bucket');

            -- Update the buckets list if it's the root container
            if bucket_name == container_name then
              redis.call('ZREM', buckets_list_key, bucket_name);
              redis.call('ZREM', 'buckets:', bucket_name);
            end;
            return;
          end;

          -- Set the bucket owner.
          -- FIXME(FVE): do some checks instead of overwriting
          redis.call('HSET', bucket_key, 'account', account);

          -- Update container info
          redis.call('HSET', container_key, 'bucket', bucket_name);

          -- Update the buckets list if it's the root container
          if bucket_name == container_name then
            redis.call('ZADD', buckets_list_key, 0, bucket_name);
            redis.call('ZADD', 'buckets:', 0, bucket_name);
          end;

          -- For container holding MPU segments, we do not want to count
          -- each segment as an object. But we still want to consider
          -- their size.
          if string.find(container_name, '+segments') then
            inc_objects = 0;
          end;

          -- Increment the counters.
          redis.call('HINCRBY', bucket_key, 'objects', inc_objects);
          redis.call('HINCRBY', bucket_key, 'bytes', inc_bytes);
          redis.call('HINCRBY', bucket_key, 'damaged_objects',
                     inc_damaged_objects);
          redis.call('HINCRBY', bucket_key, 'missing_chunks',
                     inc_missing_chunks);

          -- Finally update the modification time.
          if mtime ~= '' then
            redis.call('HSET', bucket_key, 'mtime', mtime);
          end;
        end;
    
        local akey = KEYS[1]; -- key to the account hash
        local ckey = KEYS[2]; -- key to the container hash
        local clistkey = KEYS[3]; -- key to the account's container set
        local bkey_prefix = KEYS[4]; -- prefix of the key to the bucket hash
        local blistkey = KEYS[5]; -- key to the account's bucket set
        local account_id = ARGV[1];
        local container_name = ARGV[2];
        local bucket_name = ARGV[3];
        local new_mtime = ARGV[4];
        local new_dtime = ARGV[5];
        local new_total_objects = ARGV[6];
        local new_total_bytes = ARGV[7];
        local new_total_damaged_objects = ARGV[8];
        local new_missing_chunks = ARGV[9];
        local autocreate_account = ARGV[10];
        local now = ARGV[11]; -- current timestamp
        local ckey_expiration_time = ARGV[12];
        local autocreate_container = ARGV[13];

        local account_exists = redis.call('EXISTS', akey);
        if account_exists ~= 1 then
          if autocreate_account == 'True' then
            redis.call('HSET', 'accounts:', account_id, 1);
            redis.call('HMSET', akey,
                       'id', account_id,
                       'bytes', 0,
                       'objects', 0,
                       'damaged_objects', 0,
                       'missing_chunks', 0,
                       'ctime', now);
          else
            return redis.error_reply('no_account');
          end;
        end;

        if autocreate_container == 'False' then
          local container_exists = redis.call('EXISTS', ckey);
          if container_exists ~= 1 then
            return redis.error_reply('no_container');
          end;
        end;

        local mtime = redis.call('HGET', ckey, 'mtime');
        local dtime = redis.call('HGET', ckey, 'dtime');
        local objects = redis.call('HGET', ckey, 'objects');
        local bytes = redis.call('HGET', ckey, 'bytes');
        local damaged_objects = redis.call('HGET', ckey, 'damaged_objects');
        local missing_chunks = redis.call('HGET', ckey, 'missing_chunks');

        -- When the keys do not exist redis returns false and not nil
        if dtime == false then
          dtime = '0';
        end;
        if mtime == false then
          mtime = '0';
        end;
        if objects == false then
          objects = 0;
        else
          objects = tonumber(objects);
        end;
        if bytes == false then
          bytes = 0;
        else
          bytes = tonumber(bytes);
        end;
        if damaged_objects == false then
          damaged_objects = 0;
        else
          damaged_objects = tonumber(damaged_objects);
        end;
        if missing_chunks == false then
          missing_chunks = 0;
        else
          missing_chunks = tonumber(missing_chunks);
        end;

        if autocreate_container == 'False' and is_sup(dtime, mtime) then
          return redis.error_reply('no_container');
        end;

        local old_mtime = mtime;
        local inc_objects = 0;
        local inc_bytes = 0;
        local inc_damaged_objects = 0;
        local inc_missing_chunks = 0;
        local deleted = false;

        if not is_sup(new_dtime, dtime) and not is_sup(new_mtime, mtime) then
          return redis.error_reply('no_update_needed');
        end;

        if is_sup(new_mtime, mtime) then
          mtime = new_mtime;
        end;

        if is_sup(new_dtime, dtime) then
          dtime = new_dtime;
        end;
        if is_sup(dtime, mtime) then
          -- Protect against "minus zero".
          if objects ~= 0 then
            inc_objects = -objects;
          end;
          if bytes ~= 0 then
            inc_bytes = -bytes;
          end;
          if damaged_objects ~= 0 then
            inc_damaged_objects = -damaged_objects;
          end;
          if missing_chunks ~= 0 then
            inc_missing_chunks = -missing_chunks;
          end;
          redis.call('HMSET', ckey,
                     'bytes', 0, 'objects', 0,
                     'damaged_objects', 0, 'missing_chunks', 0);
          redis.call('EXPIRE', ckey, tonumber(ckey_expiration_time));
          redis.call('ZREM', clistkey, container_name);
          deleted = true;
        elseif is_sup(mtime, old_mtime) then
          redis.call('PERSIST', ckey);
          inc_objects = tonumber(new_total_objects) - objects;
          inc_bytes = tonumber(new_total_bytes) - bytes;
          inc_damaged_objects = tonumber(
              new_total_damaged_objects) - damaged_objects
          inc_missing_chunks = tonumber(new_missing_chunks) - missing_chunks;
          redis.call('HMSET', ckey,
                     'objects', tonumber(new_total_objects),
                     'bytes', tonumber(new_total_bytes),
                     'damaged_objects', tonumber(new_total_damaged_objects),
                     'missing_chunks', tonumber(new_missing_chunks));
          redis.call('ZADD', clistkey, '0', container_name);
        else
          return redis.error_reply('no_update_needed');
        end;

        redis.call('HMSET', ckey, 'mtime', mtime,
                   'dtime', dtime, 'name', container_name);
        if inc_objects ~= 0 then
          redis.call('HINCRBY', akey, 'objects', inc_objects);
        end;
        if inc_bytes ~= 0 then
          redis.call('HINCRBY', akey, 'bytes', inc_bytes);
        end;
        if inc_damaged_objects ~= 0 then
          redis.call('HINCRBY', akey, 'damaged_objects',
                     inc_damaged_objects);
        end;
        if inc_missing_chunks ~= 0 then
          redis.call('HINCRBY', akey, 'missing_chunks',
                     inc_missing_chunks);
        end;

        local current_bucket_name = redis.call('HGET', ckey, 'bucket');
        if bucket_name == '' and current_bucket_name ~= false then
          -- Use the bucket name already registered when it is not given
          bucket_name = current_bucket_name;
        end;
        if bucket_name ~= '' then
          local bkey = bkey_prefix .. bucket_name;

          -- This container is not yet associated with this bucket.
          -- We must add all the totals in case the container already existed
          -- but didn't know its parent bucket.
          if current_bucket_name == false then
            inc_objects = new_total_objects;
            inc_bytes = new_total_bytes;
            inc_damaged_objects = new_total_damaged_objects;
            inc_missing_chunks = new_missing_chunks;
          end;

          update_bucket_stats(
              ckey, bkey, blistkey, account_id, container_name, bucket_name,
              mtime, deleted,
              inc_objects, inc_bytes, inc_damaged_objects, inc_missing_chunks);
        end;
        