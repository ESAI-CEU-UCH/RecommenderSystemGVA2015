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
