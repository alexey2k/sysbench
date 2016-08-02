pathtest = string.match(test, "(.*/)") or ""

dofile(pathtest .. "common.lua")

function thread_init(thread_id)
   set_vars()

   query_comment=""

   if (oltp_proxy_table_per_host) 
   then
      host_id= math.mod(thread_id,oltp_proxy_hosts)+1
      oltp_hosts= oltp_proxy_hosts
      query_comment="/*conn".. host_id-1 .."*/ "
   end

   if (oltp_table_per_host) then
      host_id=hosts[_G["tid_"..thread_id.."_host"]]
   end
               
   if (oltp_proxy_table_per_host or oltp_table_per_host) then

      range_max=host_id * oltp_tables_count/oltp_hosts
      range_min=(host_id-1)  * oltp_tables_count/oltp_hosts + 1
      --print("here1.1: host:" .. host_id .. " min:" .. range_min .. " max ".. range_max .. " mysql host" .. oltp_hosts)
   else
      range_min=1
      range_max=oltp_tables_count
   end                                        

   --print ("Thread_id: "..thread_id .. "conn_id" .. conn_id)


   if (((db_driver == "mysql") or (db_driver == "attachsql")) and mysql_table_engine == "myisam") then
      begin_query = "LOCK TABLES sbtest WRITE"
      commit_query = "UNLOCK TABLES"
   else
      begin_query ="BEGIN".. query_comment
      commit_query ="COMMIT".. query_comment
   end

end

function get_range_str()
   local start = sb_rand(1, oltp_table_size)
   return string.format(" WHERE id BETWEEN %u AND %u",
                        start, start + oltp_range_size - 1)
end

function event(thread_id)
   local rs
   local i
   local table_name
   local c_val
   local pad_val
   local query

   if (oltp_table_per_host or oltp_proxy_table_per_host) then
     table_name = query_comment .."sbtest".. sb_rand_uniform(range_min, range_max)
   else
     table_name = query_comment .."sbtest".. sb_rand_uniform(1, oltp_tables_count)
   end   

   if not oltp_skip_trx then
      db_query(begin_query)
   end

   for i=1, oltp_point_selects do
      rs = db_query("SELECT c FROM ".. table_name .." WHERE id=" ..
                       sb_rand(1, oltp_table_size))
   end

   for i=1, oltp_simple_ranges do
      rs = db_query("SELECT c FROM ".. table_name .. get_range_str())
   end

   for i=1, oltp_sum_ranges do
      rs = db_query("SELECT SUM(K) FROM ".. table_name .. get_range_str())
   end

   for i=1, oltp_order_ranges do
      rs = db_query("SELECT c FROM ".. table_name .. get_range_str() ..
                    " ORDER BY c")
   end

   for i=1, oltp_distinct_ranges do
      rs = db_query("SELECT DISTINCT c FROM ".. table_name .. get_range_str() ..
                    " ORDER BY c")
   end

   if not oltp_read_only then

   for i=1, oltp_index_updates do
      rs = db_query("UPDATE " .. table_name .. " SET k=k+1 WHERE id=" .. sb_rand(1, oltp_table_size))
   end

   for i=1, oltp_non_index_updates do
      c_val = sb_rand_str("###########-###########-###########-###########-###########-###########-###########-###########-###########-###########")
      query = "UPDATE " .. table_name .. " SET c='" .. c_val .. "' WHERE id=" .. sb_rand(1, oltp_table_size)
      rs = db_query(query)
      if rs then
        print(query)
      end
   end

   if oltp_test_mode then

   i = sb_rand(1, oltp_table_size)

   rs = db_query("DELETE FROM " .. table_name .. " WHERE id=" .. i)
   
   c_val = sb_rand_str([[
###########-###########-###########-###########-###########-###########-###########-###########-###########-###########]])
   pad_val = sb_rand_str([[
###########-###########-###########-###########-###########]])

   rs = db_query("INSERT INTO " .. table_name ..  " (id, k, c, pad) VALUES " .. string.format("(%d, %d, '%s', '%s')",i, sb_rand(1, oltp_table_size) , c_val, pad_val))
   end -- oltp_test_mode

   end -- oltp_read_only

   if not oltp_skip_trx then
      db_query(commit_query)
   end

end

