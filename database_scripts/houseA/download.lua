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
package.path = package.path ..";".. arg[0]:dirname().."../?.lua"
require "aprilann"
local raspimon = require "raspimon"

--
-- Problems with the data:
--
--   - 2016/02/29 - 2016/03/03 OpenEnergyMonitor was disconnected
--
-- In January there are some days with disconnection problems also.

-- os.setlocale ("", "time")
local MAC       = "74da38545ebd"
local FROM_DATE = os.time{year=2015, month=12, day=17, hour=0} -- included
local TO_DATE   = os.time{year=2016, month=02, day=29, hour=0} -- excluded
local DEST_DIR  = arg[1] or "/home/experimentos/CORPORA/RASPIMON/houseA"

raspimon.extract_data(MAC, FROM_DATE, TO_DATE, DEST_DIR)
