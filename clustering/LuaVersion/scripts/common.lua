--[[
  This file is part of RecommenderSystemGVA2015
  
  Copyright 2016, Francisco Zamora-Martinez
  
  The Lua-MapReduce toolkit is free software; you can redistribute it and/or modify it
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

-- This function connects to the database and loads all the series in MongoDB
-- which match strings in topic_matches list. This function uses PARXE engine
-- and is executed in a PBS cluster.
--
-- The function returns a dictionary of data_frame.series objects with pairs of
-- (timestamp,value)
local function load_series(topic_matches)
  local px  = require "parxe"
  px.config.set_min_task_len(1)
  
  local any = function(topic,matches)
    for i=1,#matches do
      if topic:find(matches[i]) then
        return true
      end
    end
  end
  
  local function map_fn(path)
    local timestamps = {} -- dict of matrices
    local values = {} -- dict of matrices
    -- for i,path in iterator(ipairs(days)) do
    print(path)
    local df = data_frame.from_csv(path)
    local g = df:groupby("topic")
    for _,topic in ipairs(g:levels("topic")) do
      if any(topic,topic_matches) then
        local t_df = g:get_group(topic)
        local idx = t_df:as_matrix("timestamp",{dtype="int32"})
        local m = t_df:as_matrix("value",{dtype="float"})
        timestamps[topic] = idx
        values[topic]     = m
      end
    end
    collectgarbage("collect")
    return { timestamps = timestamps, values = values }
  end
  
  print("Loading all days...")
  local days = glob("/home/experimentos/CORPORA/RASPIMON/houseA/*/*series.csv.gz")
  
  local result = px.map(map_fn, days):
    after(function(tbl)
        return iterator(tbl):
          reduce(
            function(agg, x)
              local timestamps,values = x.timestamps,x.values
              for topic in pairs(timestamps) do
                agg.timestamps[topic] = agg.timestamps[topic] or {}
                agg.values[topic] = agg.values[topic] or {}
                table.insert(agg.timestamps[topic], timestamps[topic])
                table.insert(agg.values[topic], values[topic])
              end
              return agg
            end,
            { timestamps={}, values={} }
          )
    end):get()
  
  local timestamps = result.timestamps -- dict of matrices
  local values = result.values -- dict of matrices
  
  print("Transforming into series...")
  local series = {}
  for k in pairs(timestamps) do
    local df = data_frame({ data={ timestamp = matrix.join(1, timestamps[k]),
                                   value     = matrix.join(1, values[k])} })
    series[k] = data_frame.series(df, "timestamp", "value")
  end

  print("Series ready")
  px.config.set_min_task_len(32)
  return series
end

-- This function receives a data_frame.series object (s) with the consumption
-- data, another data_frame.series object (d) with demands, a period in seconds
-- of the series sampling, the minimum value (th) in seconds to take a device as
-- ON, and a minimum time between two ON states (run) in seconds.
--
-- The objects s and d should be resampled previously from raw series by using
-- a common period value.
--
-- The function returns a dictionary (comsumption and demand fileds) with a list
-- of comsumption patterns and another list with demand patterns.
local function extract_patterns(s, d, period, th, run)
  local th,run = th or 10,run or 300
  local s = s:get_data():flatten()
  local d = d:get_data():flatten()
  local gt_b = s:gt(th)
  local gt_i = gt_b:convert_to("int32")
  local diff = gt_i[{'2:'}] - gt_i[{'1:-2'}]
  local nz_pos = diff:neq(0):to_index()
  local pats = { consumption={}, demand={} }
  if nz_pos then
    local from,to
    local n = 0
    for i=1,#nz_pos do
      local p = nz_pos[i]
      local v = diff[p]
      if v == 1 then
        if not to or (p - to)*period > run then
          from = p
          n = n + 1
        end
      elseif v == -1 and from then
        to = nz_pos[i]+1
        pats.consumption[n] = s[{ {from,to} }]
        pats.demand[n] = d[{ {from,to} }]
      end
    end
  else
    print("WARNING!!! zero legnth patterns")
  end
  return pats
end

-- Implementation of DTW algorithm for two series (potentially with different
-- lengths).
--
-- Returns the sqrt of the DTW square differences summation.
--
-- [DTW in wikipedia](https://en.wikipedia.org/wiki/Dynamic_time_warping).
local function dtw_ts(a, b)
  local min = math.min
  local costs = matrix(#a+1, #b+1)
  costs[{1,'2:'}]:fill(math.huge)
  costs[{'2:',1}]:fill(math.huge)
  costs:set(1,1,0)
  for i=2,#a+1 do
    for j=2,#b+1 do
      local ai,bj = a[i-1],b[j-1]
      local c   = (ai - bj)^2
      local del = costs:get(i,   j-1)
      local ins = costs:get(i-1, j)
      local sub = costs:get(i-1, j-1)
      costs[{i,j}] = c + min(min(del, ins), sub)
    end
  end
  return math.sqrt( costs:get(#a+1,#b+1) )
end

-- Given an array of consumption (or demand) patterns, this function returns a
-- costs matrix for every pair of patterns. Matrix costs are computed by means
-- of DTW algorithm.
local function compute_cost_matrix(pats)
  local px = require "parxe"
  if #pats == 0 then
    return px.future.conditioned(function()
        print("WARNING!!! zero legnth cost matrix")
        return matrix(1,1):zeros()
    end)
  end
  local px = require "parxe"
  local result = matrix(#pats,#pats):zeros()
  local fut = px.map(
    function(i)
      for j=i+1,#pats do
        local dist = dtw_ts(pats[i], pats[j])
        result[{i,j}] = dist
        result[{j,i}] = dist
      end
      return result
    end,
    #pats
  )
  local r = px.future.conditioned(
    function(l)
      local result = matrix(#pats,#pats):zeros()
      for i=1,#l do result:axpy(1.0, l[i]) end
      return result
    end,
    fut
  )
  return r
end


-- Using [hierarchical
-- clustering](https://en.wikipedia.org/wiki/Hierarchical_clustering).
--
-- Linkage options:
-- 
-- - **min_linkage**: Minimum or single-linkage clustering:
-- 
-- $\displaystyle \min\big\{ d(a,b) : a \in A, b \in B \big\}$
-- 
-- - **max_linkage**: Maximum or complete-linkage clustering:
-- 
-- $\displaystyle \max\big\{ d(a,b) : a \in A, b \in B \big\}$
-- 
-- - **avg_linkage**: Mean or average linkage clustering, or UPGMA:
-- 
-- $\displaystyle \frac{1}{|A||B|} \sum_{a \in A}\sum_{b \in B} d(a,b)$
-- 
-- - **min_energy**: Minimum energy clustering:
-- 
-- $\displaystyle \frac {2}{nm}\sum_{i,j=1}^{n,m} \|a_i- b_j\|_2 - \frac {1}{n^2}\sum_{i,j=1}^{n} \|a_i-a_j\|_2 - \frac{1}{m^2}\sum_{i,j=1}^{m} \|b_i-b_j\|_2$
-- 
local function avg_linkage(A,B,costs) -- Mean or average linkage clustering
  local d = costs:index(1,A):index(2,B):sum()
  return 1/(#A*#B) * d
end

local function min_linkage(A,B,costs) -- Minimum or single linkage clustering
  local aux = costs:index(1,A):index(2,B):flatten()
  aux:indexed_fill(1, aux:eq(0), math.huge)
  local d = aux:min()
  return d
end

local function max_linkage(A,B,costs) -- Maximum or complete linkage clustering
  local d = costs:index(1,A):index(2,B):max()
  return d
end

local function min_energy(A,B,costs) -- Minimum energy clustring
  local inter_cluster   = 2 * avg_linkage(A,B,costs)
  local intra_cluster_A = avg_linkage(A,A,costs)
  local intra_cluster_B = avg_linkage(B,B,costs)
  return inter_cluster - intra_cluster_A - intra_cluster_B
end


-- CH-index computing centroids as the series closest to all other series in the
-- same cluster.
local function cluster_ch_index(clusters, costs)
  local _,global_centroid = costs:sum(2):min()
  local centroids = {}
  for k=1,#clusters do
    -- print(costs:index(1,clusters[k]):index(2,clusters[k]):sum(2))
    local s = costs:index(1,clusters[k]):index(2,clusters[k]):sum(2)
    local a,b = s:squeeze():min()
    centroids[k] = clusters[k][b]
  end
  -- print(global_centroid)
  -- pprint(centroids, #clusters)
  local W = 0 -- within-cluster variation
  local B = 0 -- between-cluster variation
  for k=1,#clusters do
    W = W + costs:index(1,clusters[k]):index(2,{centroids[k]}):sum()
    B = B + #clusters[k]*costs[centroids[k]][global_centroid]
  end
  local K,n = #clusters,#costs
  return (B/(K-1)) / (W/(n-K))
end

local function generate_triu_indices(n)
  local r = {}
  for i=1,n do
    for j=i+1,n do r[#r+1] = {i,j} end
  end
  return r
end

local map_fn = function(i, costs, cl, linkage_fn, huge)
  local linkage = huge
  local candidate = {0,0}
  for j=i+1,#cl do
    local l = linkage_fn(cl[i], cl[j], costs)
    if l < linkage then
      linkage = l
      candidate[1] = i
      candidate[2] = j
    end
    if j % 200 == 0 then collectgarbage("collect") end
  end
  collectgarbage("collect")
  return { linkage = linkage,  candidate = candidate }
end

local function hierarchical_clustering(costs, linkage_fn)
  local px = require "parxe"
  if #costs == 1 then return {{{1}}},matrix(1):zeros(),matrix(1,2,{1,1}) end
  local linkage_fn = linkage_fn or min_energy
  local linkages   = matrix(#costs-1):fill(math.huge)
  local candidates = matrix(#costs-1,2)
  local clusters   = { iterator.range(#costs):map(lambda'|x|{x}'):table() }
  local all_results = {}
  for k=1,#costs-1 do
    -- look-up next candidate
    local cl = clusters[k]
    local candidate = candidates[k]
    -- local indices = generate_triu_indices(#cl)
    local best = iterator( px.map(map_fn, #cl, costs, cl, linkage_fn, math.huge):get() ):
      reduce(lambda'|agg,x|(x.linkage < agg.linkage) and x or agg',
             { linkage=math.huge })
    collectgarbage("collect")
    linkages[k]  = best.linkage
    candidate[1] = best.candidate[1]
    candidate[2] = best.candidate[2]
    -- merge
    local merged = {}
    for i=1,#cl do
      if i~=candidate[1] and i~=candidate[2] then
        merged[#merged+1] = cl[i]
      end
    end
    merged[#merged+1] = table.join(cl[candidate[1]], cl[candidate[2]])
    clusters[#clusters+1] = merged
    collectgarbage("collect")
  end
  return clusters,linkages,candidates
end

return {
  load_series = load_series,
  extract_patterns = extract_patterns,
  dtw_ts = dtw_ts,
  compute_cost_matrix = compute_cost_matrix,
  avg_linkage = avg_linkage,
  min_linkage = min_linkage,
  max_linkage = max_linkage,
  min_energy = min_energy,
  cluster_ch_index = cluster_ch_index,
  hierarchical_clustering = hierarchical_clustering,
}
