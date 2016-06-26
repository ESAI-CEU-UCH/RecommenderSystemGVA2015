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
local SESSION_PATH = "/home/PRIVATE/experimentos/RASPIMON/appliance_patterns/clustering/data"
local SESSION_NAME = "session.lua.gz"
local MAX_K = 40

local session = util.deserialize(SESSION_PATH .. "/" .. SESSION_NAME)

local topics = session.topics
local cost_matrices = session.cost_matrices
local clustering_results = session.clustering_results
local ch_indices  = {}
local ch_linkages = {}
for linkage,cl_result in pairs(clustering_results) do
  local chs = {}
  ch_indices[linkage] = chs
  for _,topic in ipairs(topics) do
    chs[topic] = { 0 }
    local clusters = cl_result[topic].clusters
    local linkages = cl_result[topic].linkages
    local costs = cost_matrices[topic]
    for i=#clusters-1,math.max(2, #clusters-MAX_K+1),-1 do
      local ch = common.cluster_ch_index(clusters[i], costs)
      chs[topic][#clusters[i]] = ch
    end
  end
end

local linkages = iterator(pairs(ch_indices)):select(1):table()
table.sort(linkages)
print(table.concat(linkages, " "))
for _,topic in ipairs(topics) do
  print("#",topic)
  for i=1,#ch_indices[linkages[1]][topic] do
    printf("%4d", i)
    for _,linkage in ipairs(linkages) do
      printf(" %20.6f", ch_indices[linkage][topic][i])
    end
    printf("\n")
  end
end
