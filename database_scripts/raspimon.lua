--[[
  This file is part of RecommenderSystemGVA2015
  
  Copyright 2016, Francisco Zamora-Martinez
  
  RecommenderSystemGVA2015 is free software; you can redistribute it and/or modify it
  under the terms of the GNU General Public License version 3 as
  published by the Free Software Foundation
  
  This library is distributed in the hope that it will be useful, but WITHOUT
  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
  FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
  for more details.
  
  You should have received a copy of the GNU General Public License
  along with this library; if not, write to the Free Software Foundation,
  Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
]]
local mongo = require "mongo"

local raspimon = {}

local RASPIMON_TOPIC_BASE="raspimon"
local FORECAST_TOPIC_BASE="forecast"
local DATA_NS = "raspimon.GVA2015_data"

function normalize(v) return tonumber(v) or ("%q"):format(tostring(v)) end

function raspimon.get_topics(topic_base, mac)
  local db = mongo.Connection.New()
  assert( db:connect("localhost:27018") )
  local query = ('{ topic:/%s.%s.*/ }'):format(topic_base, mac)
  local cursor = db:query(DATA_NS, query, nil, nil, { topic=1 })
  local topics = iterator(cursor:results()):field("topic"):map(lambda'|x|x,true'):table()
  return iterator(pairs(topics)):select(1):table()
end

function raspimon.extract_data_from_raspimon_series(topic, from_date, to_date)
  local db = mongo.Connection.New()
  assert( db:connect("localhost:27018") )
  db:drop_collection("raspimon.result")
  -- aggregate all values into one collection
  local mapfn = [[
  function() {
    var b = this.basetime;
    var t = this.topic;
    for(i=0; i<this.values.length; ++i) {
      emit(new Date(b.getTime()+this.delta_times[i]*1000.0), this.values[i]);
    }
  }
]]
  local reducefn = [[ function(key,values) { return values[0]; } ]]
  local query =
    {
      topic = topic,
      basetime =
        {
          ["$gte"] = mongo.Date(tostring(from_date).."000"),
          ["$lt"]  = mongo.Date(tostring(to_date).."000"),
        }
    }
  assert( db:mapreduce("raspimon.GVA2015_data", mapfn, reducefn, query, "result") )
  -- write result as a data_frame
  local cur = assert( db:query("raspimon.result", "{query:{},orderBy:{_id:1}}") )
  local time,data = {},{}
  for i,x in iterator(cur:results()):enumerate() do
    time[i] = x._id()/1000.0 -- timestamp in seconds
    data[i] = normalize(x.value) or '"NULL"'
  end
  collectgarbage("collect")
  return time,data
end

-- "_id": ObjectID("56962148d4aaa75353880ffa"),
--    "timestamp": ISODate("2016-01-13T07:23:01.000Z"),
--    "periods_start": [
--        ISODate("2016-01-12T23:00:00.000Z"),
--        ISODate("2016-01-13T05:00:00.000Z"),
--        ISODate("2016-01-13T11:00:00.000Z"),
--        ISODate("2016-01-13T17:00:00.000Z"),
--        ISODate("2016-01-13T23:00:00.000Z"),
--        ISODate("2016-01-14T05:00:00.000Z"),
--        ISODate("2016-01-14T11:00:00.000Z"),
--        ISODate("2016-01-14T17:00:00.000Z"),
--        ISODate("2016-01-14T23:00:00.000Z"),
--        ISODate("2016-01-15T11:00:00.000Z"),
--        ISODate("2016-01-15T23:00:00.000Z"),
--        ISODate("2016-01-16T11:00:00.000Z")
--    ],
--    "topic": "forecast.74da38545ebd.aemet.daily.snow_level.46250",
--    "periods_end": [
--        ISODate("2016-01-13T05:00:00.000Z"),
--        ISODate("2016-01-13T11:00:00.000Z"),
--        ISODate("2016-01-13T17:00:00.000Z"),
--        ISODate("2016-01-13T23:00:00.000Z"),
--        ISODate("2016-01-14T05:00:00.000Z"),
--        ISODate("2016-01-14T11:00:00.000Z"),
--        ISODate("2016-01-14T17:00:00.000Z"),
--        ISODate("2016-01-14T23:00:00.000Z"),
--        ISODate("2016-01-15T11:00:00.000Z"),
--        ISODate("2016-01-15T23:00:00.000Z"),
--        ISODate("2016-01-16T11:00:00.000Z"),
--        ISODate("2016-01-16T23:00:00.000Z")
--    ],
--    "values": [
--        null,
--        null,
--        null,
--        null,
--        null,
--        null,
--        null,
--        null,
--        1300,
--        900,
--        null,
--        null
--    ],
--    "house": "MALVA"

function raspimon.extract_data(mac, from_date, to_date, dest_dir)
  local dest_dir = dest_dir or "."
  
  local write_to_dest = function(date_str, filename, header, func, topics, ...)
    local arg = table.pack(...)
    if not io.open(filename) then
      local f = assert( io.open(filename, "w") )
      local ok,msg = xpcall(
        function()
          if #header > 0 then f:write(header.."\n") end
          for i,top in ipairs(topics) do
            local time,data = func(top, table.unpack(arg))
            for j=1,#time do
              f:write(('%s,%q,%s\n'):format(time[j],top,data[j]))
            end
            printf("\r%s %7.0f%%", date_str, i/#topics*100)
            io.stdout:flush()
          end
        end, debug.traceback
      )
      f:close()
      if ok then
        printf("\r%s %7.0f%%\n", date_str, 100)
      else
        printf("\r%s failed\n", date_str)
        print(msg)
        os.remove(filename)
      end
    else
      printf("%s skipped\n", date_str)
    end
  end
  
  local raspimon_topics = raspimon.get_topics(RASPIMON_TOPIC_BASE, mac)
  local dt = 24 * 3600
  for t=from_date,to_date-dt,dt do
    local date_str = os.date("%Y%m%d",t)
    os.execute("mkdir "..dest_dir.."/"..date_str.." 2>/dev/null")
    local filename = dest_dir.."/"..date_str .. "/"..date_str.."_series.csv.gz"
    write_to_dest(date_str, filename, "timestamp,topic,value",
                  raspimon.extract_data_from_raspimon_series, raspimon_topics, t, t+dt)
  end

  -- local forecast_topics = raspimon.get_topics(FORECAST_TOPIC_BASE, mac)
  -- local dt = 24 * 3600
  -- for t=from_date,to_date,dt do
  --   local date_str = os.date("%Y%m%d",t)
  --   os.execute("mkdir "..dest_dir.."/"..date_str.." 2>/dev/null")
  --   local filename = dest_dir.."/"..date_str .. "/"..date_str.."_series.csv.gz"
  --   write_to_dest(filename, "timestamp,topic,value",
  --                 raspimon.extract_data_from_raspimon_series, raspimon_topics, t, t+dt)
  -- end
end

return raspimon
