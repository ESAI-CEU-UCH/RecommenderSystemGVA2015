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
require "aprilann"
local px = require "parxe"
px.config.set_engine("local")
--
local basedir = arg[0]:dirname()
package.path = package.path .. ";" .. basedir .. "?.lua"
--
local common = require "common"

-- Configure the default period for resampling.
local PERIOD = 60 -- 60 seconds

-- Load series captured from plugwise devices using the 8s option.

-- WARNING: THIS PROCEDURE IS REALLY SLOW!!!
local raw_data = common.load_series({ ".*power8s.*" })

-- Resample all loaded series following **rectangle** and **max** methods, so we
-- can compute **consumption** and **demand** resampled series.
local consumption = {}
local demand = {}
for k,v in pairs(raw_data) do
  consumption[k] = v:resampleU(PERIOD, {method="rectangle"})
  demand[k] = v:resampleU(PERIOD, {method="max"})
end

-- Below a table with all available topics is generated and shown.
local topics = iterator(pairs(consumption)):select(1):table()
table.sort(topics)
for i,k in ipairs(topics) do print(i,k) end

-- Extract sequences of data from resampled series so we can create patterns
-- isolating when the device has been used. See `extract_patterns()` function
-- for more information about what is a *pattern*.
local patterns = {}
for i=1,#topics do
  patterns[topics[i]] = common.extract_patterns(consumption[topics[i]],
                                                demand[topics[i]],
                                                PERIOD)
end

-- Use DTW to compute cost matrix for each serie. This computation is performed
-- in parallel because it is very time consuming.
local cost_matrices_futures = {}
for k,t in ipairs(topics) do
  cost_matrices_futures[k] = common.compute_cost_matrix(patterns[t].consumption)
end
local cost_matrices = px.future.conditioned(
  function(t) return iterator.zip(iterator(topics), iterator(t)):table() end,
  px.future.all(cost_matrices_futures) ):get()

-- Execute hierarchical clustering algorithm. For every serie (topic) the
-- clustring algorithms returns the sequence of clusters for every step of the
-- algorithm, the linkage distances of every merge, and the cluster candidates
-- considered at merge step.
local clustering_results = {}
for _,t in ipairs{
  {common.min_linkage,"min_linkage"},
  {common.max_linkage, "max_linkage"},
  {common.avg_linkage, "avg_linkage"},
  {common.min_energy; "min_energy"},
} do
  print("Clustring: ", t[2])
  local linkage = t[1]
  local results = {}
  for k=1,#topics do
    local t = topics[k]
    print("",t)
    local costs = cost_matrices[t]
    local clusters,linkages,candidates = common.hierarchical_clustering(costs, linkage)
    results[t] = {}
    results[t].clusters   = clusters
    results[t].linkages   = linkages
    results[t].candidates = candidates
  end
  clustering_results[t[2]] = results
end

local dir = "/home/experimentos/RASPIMON/appliance_patterns/clustering/data/"
util.serialize(
  {
    topics = topics,
    cost_matrices = cost_matrices,
    consumption = consumption,
    demand = demand,
    patterns = patterns,
    clustering_results = clustering_results,
  },
  dir .. "session.lua.gz"
)

local f = io.open(dir .. "session.content.txt", "w")
f:write([[{
    topics = topics,
    cost_matrices = cost_matrices,
    consumption = consumption,
    demand = demand,
    patterns = patterns,
    clustering_results = {
      max_linkage = max_clustering_results,
      min_linkage = min_clustering_results,
      avg_linkage = avg_clustering_results,
      min_energy = min_energy_clustring_results,
    }
}
]])
f:close()
