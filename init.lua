-- init.lua  (ps_tracker)
-- DanNet-aware Power Source tracker for all toons.
--
-- Features:
--   * Scans equipped PS and bags every 5s; broadcasts state to all toons via DanNet actors
--   * ImGui overlay: each toon's PS name, tier, power % (or charges), and status
--   * Auto-swaps a spent PS for the best fresh one in bags (role-matched, tier-sorted)
--   * Auto-converts sealed PS items via ItemDisplayWindow before equipping
--   * Auto-accepts incoming PS trades and equips the received item
--   * Trade/Swap buttons in the UI to coordinate PS distribution across the group
--
-- Install:  copy the ps_tracker/ folder into MQ/lua/
-- Run on every toon:  /lua run ps_tracker
-- Commands:
--   /ps stop      — stop the script
--   /ps ui        — toggle the ImGui window
--   /ps swap      — manually trigger a PS swap now
--   /ps scan      — force immediate re-scan and broadcast
--   /ps convert   — right-click convert the first sealed PS in bags

local mq    = require('mq')
local imgui = require('ImGui')

local actors
do
    local ok, mod = pcall(require, 'actors')
    if ok then
        actors = mod
    else
        printf('\ay[PS]\ax actors module unavailable — running in single-toon mode')
    end
end

-- ============================================================
-- Power Source database
-- Add new expansion items here as they release.
-- tier : higher = better
-- role : melee | tank | caster | healer | all
-- sealed : true  → stackable Misc item that needs right-click convert
--                   before it can be equipped; not a valid swap candidate
-- ============================================================
local PS_DB = {
    -- Tier 11 — Shattering of Ro: Riven Arcana (current highest, level 129)
    -- Uses "Protector" for tank (same convention as Emberquartz, not Luclinite's "Defender")
    ["Riven Arcana Slayer Source"]             = { tier=11, role='melee'  },
    ["Riven Arcana Protector Source"]          = { tier=11, role='tank'   },
    ["Riven Arcana Conjuror Source"]           = { tier=11, role='caster' },
    ["Riven Arcana Wayfarer Source"]           = { tier=11, role='healer' },
    -- Sealed variants (need right-click convert before they can be equipped)
    ["Sealed Riven Arcana Slayer Source"]      = { tier=11, role='melee',  sealed=true },
    ["Sealed Riven Arcana Protector Source"]   = { tier=11, role='tank',   sealed=true },
    ["Sealed Riven Arcana Conjuror Source"]    = { tier=11, role='caster', sealed=true },
    ["Sealed Riven Arcana Wayfarer Source"]    = { tier=11, role='healer', sealed=true },
    -- Tier 10 — Laurion's Song: Unparalleled (seasonal/quest)
    ["Unparalleled Slayer Source"]    = { tier=10, role='melee'  },
    ["Unparalleled Defender Source"]  = { tier=10, role='tank'   },
    ["Unparalleled Conjuror Source"]  = { tier=10, role='caster' },
    ["Unparalleled Wayfarer Source"]  = { tier=10, role='healer' },
    -- Tier 9 — Laurion's Song: Emberquartz (standard current)
    ["Emberquartz Slayer Source"]     = { tier=9,  role='melee'  },
    ["Emberquartz Protector Source"]  = { tier=9,  role='tank'   },
    ["Emberquartz Conjuror Source"]   = { tier=9,  role='caster' },
    ["Emberquartz Wayfarer Source"]   = { tier=9,  role='healer' },
    -- Tier 8 — Terror of Luclin: Luclinite
    ["Luclinite Attacker Source"]     = { tier=8,  role='melee'  },
    ["Luclinite Defender Source"]     = { tier=8,  role='tank'   },
    ["Luclinite Convoker Source"]     = { tier=8,  role='caster' },
    ["Luclinite Mendicant Source"]    = { tier=8,  role='healer' },
    -- Tier 7 — Night of Shadows: Shadowstone (verify exact names in-game)
    ["Shadowstone Attacker Source"]   = { tier=7,  role='melee'  },
    ["Shadowstone Defender Source"]   = { tier=7,  role='tank'   },
    ["Shadowstone Convoker Source"]   = { tier=7,  role='caster' },
    ["Shadowstone Mendicant Source"]  = { tier=7,  role='healer' },
    -- Tier 5-4 — older generic sources
    ["Xorbb's Caged Hatred"]          = { tier=5,  role='all'    },
    ["Thaell Ew Totem"]               = { tier=4,  role='all'    },
    ["Ocean Purification Stone"]      = { tier=4,  role='all'    },
    ["Geomantic Steelstone"]          = { tier=4,  role='all'    },
    ["Combine Power Shard"]           = { tier=4,  role='all'    },
    -- Tier 3 — Ring of Scale / earlier
    ["Cliff Stone"]                   = { tier=3,  role='all'    },
    ["Enchanted Dragon Scale"]        = { tier=3,  role='all'    },
    ["Scorpikis Worry Stone"]         = { tier=3,  role='all'    },
    ["Magma Stone"]                   = { tier=3,  role='all'    },
}

local CLASS_ROLE = {
    WAR='tank',   PAL='tank',   SHD='tank',
    BER='melee',  ROG='melee',  MNK='melee',
    RNG='melee',  BST='melee',  BRD='melee',
    WIZ='caster', MAG='caster', NEC='caster', ENC='caster',
    CLR='healer', DRU='healer', SHM='healer',
}

-- Fallback role preference order when no exact-role PS is available.
-- First entry is always the toon's own role (the primary pass).
local ROLE_FALLBACK = {
    melee  = {'melee',  'all', 'tank',   'healer', 'caster'},
    tank   = {'tank',   'all', 'melee',  'healer', 'caster'},
    caster = {'caster', 'all', 'healer', 'melee',  'tank'  },
    healer = {'healer', 'all', 'caster', 'melee',  'tank'  },
}

-- ============================================================
-- Runtime state
-- ============================================================
local running       = true
local showUI        = true
local peers         = {}
local myName        = mq.TLO.Me.Name()
local myClass       = mq.TLO.Me.Class.ShortName()
local myRole        = CLASS_ROLE[myClass] or 'melee'
local myServer      = mq.TLO.EverQuest.Server() or ''
local equippedCache = nil
local bagsCache     = {}
local lastScan      = 0
local lastBroadcast = 0

local SCAN_MS        = 5000
local BCAST_MS       = 5000

-- Trade automation state machine
local tradeSM = {
    active    = false,
    destName  = nil,   -- toon to trade TO
    itemName  = nil,   -- item to give
    phase     = 'idle', -- idle/nav/open/place/confirm
    nextTick  = 0,
    openTries = 0,
}
local PEER_TIMEOUT_S = 30

-- Auto-accept / auto-equip state (incoming trade)
local tradeRecvAt      = 0      -- mq.gettime() when we first detected ourselves as recipient
local tradeAccepted    = false  -- true after we clicked Accept; cleared when TradeWnd closes
local postTradeEquipAt = 0      -- mq.gettime() after which we scan bags and auto-equip

-- Non-blocking equip state machine (phases driven by main loop)
-- phase 0=idle, 1=pickup_sent, 2=equip_sent, 3=inventory_sent
local equipSM = { phase=0, best=nil, nextAt=0, convertBtnClicked=false, convertAttempts=0 }

-- ============================================================
-- Utility
-- ============================================================
local function psTier(name)   return PS_DB[name] and PS_DB[name].tier   or 0     end
local function psRole(name)   return PS_DB[name] and PS_DB[name].role            end
local function psSealed(name) return PS_DB[name] and PS_DB[name].sealed or false end

local function roleMatches(itemRole, classRole)
    return itemRole == 'all' or itemRole == classRole
end

-- Modern PS items (Riven Arcana, Emberquartz, etc.) use a Power mechanic:
-- item.Power() returns a raw 0..MaxPower value (NOT a percentage).
-- powerStr() converts to a percentage by dividing Power/MaxPower*100.
-- Old Buried Sea orbs use item.Charges() instead (they have no MaxPower).
-- We use MaxPower() > 0 to detect which mechanic applies, because Power()
-- may return 0 on non-power-source items rather than nil.
local function isItemSpent(item)
    local maxPower = item.MaxPower()
    -- MaxPower > 0 indicates a modern power-mechanic item
    if maxPower ~= nil and maxPower > 0 then
        return (item.Power() or 0) == 0
    end
    -- Old Buried Sea orbs: fall back to Charges
    local ch = item.Charges()
    return type(ch) == 'number' and ch == 0
end

-- ============================================================
-- Scan own equipped Power Source slot
-- ============================================================
local function getEquippedPS()
    local item = mq.TLO.Me.Inventory('powersource')
    if not item or not item() then return nil end
    local name = item.Name()
    if not name then return nil end
    local power    = item.Power()    -- raw power points, nil for old-style orbs
    local maxPower = item.MaxPower() -- max power points, nil for old-style orbs
    local ch       = item.Charges()  -- for old Buried Sea orbs only
    return {
        name     = name,
        tier     = psTier(name),
        role     = psRole(name) or 'unknown',
        power    = power,
        maxPower = maxPower,
        charges  = (ch ~= nil) and ch or -1,
        spent    = isItemSpent(item),
        sealed   = psSealed(name),
    }
end

-- ============================================================
-- Scan bags for all known PS items (including sealed variants)
-- Returns table sorted: fresh non-sealed first, then fresh sealed,
-- then spent — within each group by tier descending.
-- ============================================================
local function getPSInBags()
    local found = {}
    for bag = 1, 10 do
        local pack = mq.TLO.Me.Inventory(('pack%d'):format(bag))
        if pack and pack() then
            local topName = pack.Name()
            if topName and PS_DB[topName] then
                -- PS item sitting directly in the pack slot (not inside a bag)
                local power    = pack.Power()
                local maxPower = pack.MaxPower()
                local ch       = pack.Charges()
                local sealed   = psSealed(topName)
                table.insert(found, {
                    name     = topName,
                    bag      = bag,
                    slot     = 0,   -- sentinel: item sits directly in the top-level pack slot
                    tier     = psTier(topName),
                    role     = psRole(topName),
                    power    = power,
                    maxPower = maxPower,
                    charges  = (ch ~= nil) and ch or -1,
                    spent    = not sealed and isItemSpent(pack),
                    sealed   = sealed,
                })
            elseif (pack.Container() or 0) > 0 then
                for slot = 1, pack.Container() do
                    local item = pack.Item(slot)
                    if item and item() then
                        local name = item.Name()
                        if PS_DB[name] then
                            local power    = item.Power()
                            local maxPower = item.MaxPower()
                            local ch       = item.Charges()
                            local sealed   = psSealed(name)
                            table.insert(found, {
                                name     = name,
                                bag      = bag,
                                slot     = slot,
                                tier     = psTier(name),
                                role     = psRole(name),
                                power    = power,
                                maxPower = maxPower,
                                charges  = (ch ~= nil) and ch or -1,
                                spent    = not sealed and isItemSpent(item),
                                sealed   = sealed,
                            })
                        end
                    end
                end
            end
        end
    end

    -- Sort: non-sealed fresh first (best tier), then sealed fresh, then spent
    table.sort(found, function(a, b)
        -- spent items last
        if a.spent ~= b.spent then return not a.spent end
        -- among fresh: non-sealed before sealed
        if a.sealed ~= b.sealed then return not a.sealed end
        -- within same group: higher tier first
        return a.tier > b.tier
    end)
    return found
end

-- Strip bag/slot coords before broadcasting (peers need stats, not positions)
local function sanitizeBags(items)
    local out = {}
    for _, v in ipairs(items) do
        table.insert(out, {
            name     = v.name,
            tier     = v.tier,
            role     = v.role,
            power    = v.power,
            maxPower = v.maxPower,
            charges  = v.charges,
            spent    = v.spent,
            sealed   = v.sealed,
        })
    end
    return out
end

-- ============================================================
-- Auto-swap / auto-equip: equip the best fresh PS for this toon.
-- Handles two cases:
--   A) Slot empty (equippedCache == nil):
--      2-step equip — pick up from bag, equip directly (no old PS to stow).
--   B) Slot has a spent PS:
--      3-step swap — pick up spent from slot, click bag slot of fresh PS
--      (EQ swaps them: spent lands in bag, fresh on cursor), equip fresh.
-- Sealed items are SKIPPED — they cannot be equipped directly.
-- ============================================================
local function trySwapPS(force, strict)
    -- Needs action if forced, slot is empty, or equipped PS is spent
    local needsAction = force or (not equippedCache) or equippedCache.spent
    if not needsAction then return false end

    local best = nil
    local usedFallback = false
    local fallbackRole = nil

    if strict then
        -- Strict mode: only accept an exact role match or 'all'.
        -- Used by the Swap button so we don't settle for a wrong-role PS.
        for _, item in ipairs(bagsCache) do
            if not item.spent and not item.sealed
               and (item.role == myRole or item.role == 'all') then
                best = item
                break
            end
        end
    else
        -- Normal mode: walk the full ROLE_FALLBACK chain (any PS beats none).
        local priorities = ROLE_FALLBACK[myRole] or {myRole, 'all'}
        for _, tryRole in ipairs(priorities) do
            for _, item in ipairs(bagsCache) do
                if not item.spent and not item.sealed and item.role == tryRole then
                    best = item
                    break
                end
            end
            if best then
                usedFallback = (tryRole ~= myRole)
                fallbackRole = tryRole
                break
            end
        end
    end

    if not best then
        printf('\ay[PS]\ax \arAuto-equip: no fresh non-sealed PS found in bags.\ax')
        return false
    end
    if usedFallback then
        printf('\ay[PS]\ax \ayNo %s PS available — equipping \at%s\ax (T%d, %s) as stopgap.\ax',
               myRole, best.name, best.tier, fallbackRole)
    end

    if not equippedCache then
        -- Case A: slot is empty — 2-step equip
        printf('\ay[PS]\ax Equipping \ag%s\ax (T%d) from bags (slot was empty)',
               best.name, best.tier)

        if best.slot == 0 then
            mq.cmdf('/itemnotify pack%d leftmouseup', best.bag)
        else
            mq.cmdf('/itemnotify in pack%d %d leftmouseup', best.bag, best.slot)
        end
        mq.delay('300ms')
        if not mq.TLO.Cursor() then
            printf('\ar[PS]\ax Equip failed: could not pick up from bag.')
            return false
        end

        mq.cmdf('/ctrl /itemnotify powersource leftmouseup')
        mq.delay('200ms')
        -- Some items (e.g. Luclinite) still show an attune confirmation dialog
        -- even with /ctrl.  Click Yes if it appears.
        if mq.TLO.Window('ConfirmationDialogBox').Open() then
            printf('\ay[PS]\ax Attune confirmation dialog — clicking Yes')
            mq.TLO.Window('ConfirmationDialogBox').Child('CD_Yes_Button').LeftMouseUp()
            mq.delay('300ms')
        end

        if mq.TLO.Cursor() then
            printf('\ar[PS]\ax Equip anomaly: cursor still has an item after equip!')
            return false
        end

        printf('\ay[PS]\ax \agEquip complete! Now wearing %s.\ax', best.name)
        return true
    else
        -- Case B: slot has a spent PS — 3-step swap
        printf('\ay[PS]\ax Swapping \ar%s\ax (spent) → \ag%s\ax (T%d)',
               equippedCache.name, best.name, best.tier)

        -- /ctrl required to pick up an equipped item from the slot onto cursor
        mq.cmdf('/ctrl /itemnotify powersource leftmouseup')
        mq.delay('300ms')
        if not mq.TLO.Cursor() then
            printf('\ar[PS]\ax Swap failed: could not pick up equipped PS.')
            return false
        end

        if best.slot == 0 then
            mq.cmdf('/itemnotify pack%d leftmouseup', best.bag)
        else
            mq.cmdf('/itemnotify in pack%d %d leftmouseup', best.bag, best.slot)
        end
        mq.delay('300ms')

        mq.cmdf('/ctrl /itemnotify powersource leftmouseup')
        mq.delay('200ms')
        -- Some items (e.g. Luclinite) still show an attune confirmation dialog
        -- even with /ctrl.  Click Yes if it appears.
        if mq.TLO.Window('ConfirmationDialogBox').Open() then
            printf('\ay[PS]\ax Attune confirmation dialog — clicking Yes')
            mq.TLO.Window('ConfirmationDialogBox').Child('CD_Yes_Button').LeftMouseUp()
            mq.delay('300ms')
        end

        if mq.TLO.Cursor() then
            printf('\ar[PS]\ax Swap anomaly: cursor still has an item!')
            return false
        end

        printf('\ay[PS]\ax \agSwap complete! Now wearing %s.\ax', best.name)
        return true
    end
end

-- ============================================================
-- DanNet Actors
-- ============================================================
-- Declared here (before the mailbox closure) so the callback captures
-- this local as an upvalue rather than writing to a global.
local pendingForceSwap  = false  -- set true to trigger a swap regardless of spent state

local mailbox = nil
if actors then
    mailbox = actors.register('ps_tracker', function(message)
        if not message or not message.content then return end
        local d = message.content
        if type(d) ~= 'table' then return end
        -- Actors broadcast across all MQ instances, including different servers.
        -- Discard messages from other servers (e.g. Live vs EMU on same machine).
        if d.server ~= myServer then return end
        -- Trade request: {type='do_trade', dest=name, item=name}
        if d.type == 'do_trade' and d.dest and d.item then
            tradeSM.active    = true
            tradeSM.destName  = d.dest
            tradeSM.itemName  = d.item
            tradeSM.phase     = 'nav'
            tradeSM.nextTick  = 0
            tradeSM.openTries = 0
            printf('\ay[PS]\ax Trade request: give \at%s\ax to \at%s', d.item, d.dest)
            return
        end
        -- Swap request: {type='do_swap'}
        if d.type == 'do_swap' then
            pendingForceSwap = true
            printf('\ay[PS]\ax Swap request received for \at%s\ax — searching for %s PS on next tick',
                   myName, myRole)
            return
        end
        -- Normal status broadcast
        if not d.name then return end
        if d.name == myName then return end
        d.ts = os.time()
        peers[d.name] = d
    end)
end

local function buildStatus()
    return {
        server   = myServer,
        name     = myName,
        class    = myClass,
        role     = myRole,
        equipped = equippedCache,
        bags     = sanitizeBags(bagsCache),
    }
end

local function broadcastStatus()
    if not mailbox then return end
    mailbox:send({ script='ps_tracker', mailbox='ps_tracker' }, buildStatus())
end

-- ============================================================
-- Find who can supply a PS to a needy toon
-- Prefers non-sealed items; falls back to sealed (with sealed=true flag).
-- Returns: { peer=name, item=item, sealed=bool } or nil
-- ============================================================
local function findTradeSource(forRole)
    local best      = nil   -- best non-sealed fresh item from self or a peer
    local bestSeal  = nil   -- best sealed fresh item from self or a peer (fallback)
    local now       = os.time()

    -- Check local bags (bagsCache always reflects the most recent scan)
    for _, item in ipairs(bagsCache) do
        if not item.spent and roleMatches(item.role, forRole) then
            if not item.sealed then
                if not best or item.tier > best.item.tier then
                    best = { peer = myName, item = item, sealed = false }
                end
            else
                if not bestSeal or item.tier > bestSeal.item.tier then
                    bestSeal = { peer = myName, item = item, sealed = true }
                end
            end
        end
    end

    -- Check peers
    for _, peer in pairs(peers) do
        if peer.name ~= myName and (now - (peer.ts or 0)) <= PEER_TIMEOUT_S then
            for _, item in ipairs(peer.bags or {}) do
                if not item.spent and roleMatches(item.role, forRole) then
                    if not item.sealed then
                        if not best or item.tier > best.item.tier then
                            best = { peer = peer.name, item = item, sealed = false }
                        end
                    else
                        if not bestSeal or item.tier > bestSeal.item.tier then
                            bestSeal = { peer = peer.name, item = item, sealed = true }
                        end
                    end
                end
            end
        end
    end
    -- Non-sealed preferred; sealed only if nothing better exists
    return best or bestSeal
end

-- ============================================================
-- DanNet remote execute helper — module level so renderUI and
-- the main loop can both call it.
-- ============================================================
local pendingRestartAt  = 0      -- epoch ms; > 0 means a peer-restart is queued
-- (pendingForceSwap is declared earlier, before the mailbox closure)

local function dexAll(cmd)
    -- mq.TLO.DanNet is nil when MQ2DanNet is not loaded
    if not mq.TLO.DanNet then
        printf('\ay[PS]\ax DanNet plugin not loaded.')
        return
    end
    local peersStr = mq.TLO.DanNet.Peers()
    if not peersStr or peersStr == '' then return end
    for peer in peersStr:gmatch('[^|]+') do
        peer = peer:match('^%s*(.-)%s*$')
        if peer and peer ~= '' then
            mq.cmdf('/dex %s %s', peer, cmd)
        end
    end
end

-- ============================================================
-- Auto-accept incoming trade + auto-equip after receipt.
-- Called each main-loop tick — fully non-blocking.
-- Equip is driven by equipSM (phase 0-3) so no inline mq.delay().
-- ============================================================
local function autoAcceptStep()
    local now       = mq.gettime()
    local tradeWnd  = mq.TLO.Window('TradeWnd')
    local tradeOpen = tradeWnd and tradeWnd.Open()

    -- ---- Auto-accept ----------------------------------------
    if not tradeOpen then
        -- Window closed: clear all per-trade flags
        tradeRecvAt   = 0
        tradeAccepted = false
    elseif not tradeSM.active and not tradeAccepted then
        -- TradeWnd is open but WE did not initiate it → we are the recipient
        if tradeRecvAt == 0 then
            tradeRecvAt = now          -- start the 1-second delay timer
        elseif now - tradeRecvAt >= 1000 then
            local tradeBtn = tradeWnd.Child('TRDW_Trade_Button')
            if tradeBtn and tradeBtn() then
                printf('\ay[PS]\ax Auto-accepting incoming trade')
                tradeBtn.LeftMouseUp()
                postTradeEquipAt = now + 2000  -- schedule bag-scan 2 s after accept
            end
            tradeAccepted = true   -- prevent re-triggering while window fades out
            tradeRecvAt   = 0
        end
    end

    -- ---- Auto-equip phases 0-3 (pcall-wrapped so a crash inside logs an
    --      error message but does NOT kill the whole script)
    local _equipOk, _equipErr = pcall(function()

    -- ---- Auto-equip phase 0: scan + send pickup command -----
    if equipSM.phase == 0 and postTradeEquipAt > 0 and now >= postTradeEquipAt then
        postTradeEquipAt = 0

        -- Refresh caches so we see the newly received item
        equippedCache = getEquippedPS()
        bagsCache     = getPSInBags()

        -- Current equipped tier (treat spent as tier 0 so any fresh replaces it)
        local curTier = (equippedCache and not equippedCache.spent)
                        and equippedCache.tier or 0

        -- Find the best non-spent PS for our role that beats current.
        -- Sealed items are included; a convert phase handles them before equip.
        local best = nil
        for _, item in ipairs(bagsCache) do
            if not item.spent and roleMatches(item.role, myRole) then
                if item.tier > curTier then
                    best = item
                    break   -- bagsCache is already sorted best-first (non-sealed before sealed)
                end
            end
        end

        if best then
            -- Guard Cursor TLO before reading — nil-safe even if TLO is unregistered
            local cursorBefore = (mq.TLO.Cursor and mq.TLO.Cursor()) or nil
            printf('\ay[PS]\ax Auto-equip: cursor before pickup = %s',
                   tostring(cursorBefore or 'empty'))
            -- If cursor already holds an item, abort: picking up another item
            -- with a full cursor drops or merges items unpredictably.
            if cursorBefore then
                printf('\ar[PS]\ax Auto-equip: cursor is not empty (%s) before pickup — aborting.',
                       tostring(cursorBefore))
                return
            end
            equipSM.best = best
            if best.sealed then
                -- Sealed item: open ItemDisplayWindow via Inspect(), then phase 1
                -- clicks IDW_ConvertButton to convert it.  The old rightmouseup approach
                -- failed with "Could not find slot to send notification to" because
                -- /itemnotify in packN slotM requires the bag window to be open;
                -- Inspect() bypasses that restriction entirely.
                printf('\ay[PS]\ax Auto-equip: converting sealed \ag%s\ax (T%d) in pack%d slot%d',
                       best.name, best.tier, best.bag, best.slot)
                local packItem
                if best.slot == 0 then
                    packItem = mq.TLO.Me.Inventory(('pack%d'):format(best.bag))
                else
                    packItem = mq.TLO.Me.Inventory(('pack%d'):format(best.bag)).Item(best.slot)
                end
                if packItem and packItem() then
                    packItem.Inspect()
                    equipSM.phase           = 1
                    equipSM.nextAt          = now + 400  -- wait for ItemDisplayWindow to open
                    equipSM.convertBtnClicked = false
                    equipSM.convertAttempts   = 0
                else
                    printf('\ar[PS]\ax Auto-equip: cannot access item TLO for conversion — aborting.')
                end
            else
                -- Non-sealed: pick up directly
                if best.slot == 0 then
                    printf('\ay[PS]\ax Auto-equip: picking up \ag%s\ax (T%d) from pack%d (top-level)',
                           best.name, best.tier, best.bag)
                else
                    printf('\ay[PS]\ax Auto-equip: picking up \ag%s\ax (T%d) from pack%d slot%d',
                           best.name, best.tier, best.bag, best.slot)
                end
                -- Pick the new PS up from the bag slot onto the cursor
                if best.slot == 0 then
                    mq.cmdf('/itemnotify pack%d leftmouseup', best.bag)
                else
                    mq.cmdf('/itemnotify in pack%d %d leftmouseup', best.bag, best.slot)
                end
                equipSM.phase  = 2          -- cursor-check + equip phase (skips convert)
                equipSM.nextAt = now + 400  -- wait for pickup animation
            end
        else
            printf('\ay[PS]\ax Auto-equip: no upgrade found in bags (curTier=%d, role=%s)',
                   curTier, myRole)
        end
    end

    -- ---- Auto-equip phase 1: convert sealed item via ItemDisplayWindow ----
    -- Step A: wait for the window to open, then click IDW_ConvertButton.
    -- Step B: once convertBtnClicked is true, rescan bags for the unsealed item
    -- and pick it up.
    if equipSM.phase == 1 and now >= equipSM.nextAt then
        if not equipSM.convertBtnClicked then
            -- Step A: click the convert button
            local idw = mq.TLO.Window('ItemDisplayWindow')
            if idw and idw.Open() then
                local btn = idw.Child('IDW_ConvertButton')
                if btn and btn.Enabled() then
                    printf('\ay[PS]\ax Auto-equip: clicking IDW_ConvertButton to convert sealed item')
                    btn.LeftMouseUp()
                    idw.DoClose()
                    equipSM.convertBtnClicked = true
                    equipSM.convertAttempts   = 0
                    equipSM.nextAt            = now + 1500  -- wait for conversion to complete
                else
                    printf('\ar[PS]\ax Auto-equip: IDW_ConvertButton not found/enabled in ItemDisplayWindow — aborting.')
                    idw.DoClose()
                    equipSM.phase           = 0
                    equipSM.best            = nil
                    equipSM.convertBtnClicked = false
                    equipSM.convertAttempts   = 0
                end
            else
                -- Window not open yet — keep polling (up to ~3 s)
                equipSM.convertAttempts = equipSM.convertAttempts + 1
                if equipSM.convertAttempts > 10 then
                    printf('\ar[PS]\ax Auto-equip: ItemDisplayWindow did not open (timeout) — aborting.')
                    equipSM.phase           = 0
                    equipSM.best            = nil
                    equipSM.convertBtnClicked = false
                    equipSM.convertAttempts   = 0
                else
                    equipSM.nextAt = now + 300  -- poll every 300 ms
                end
            end
        else
            -- Step B: conversion complete — EQ may have placed the item on the cursor
            -- directly (common behavior), or it may be back in the original bag slot.
            local cursor = mq.TLO.Cursor and mq.TLO.Cursor()
            if cursor then
                -- Already on cursor — skip pickup, go straight to equip
                printf('\ay[PS]\ax Auto-equip: converted item on cursor (%s) — equipping', cursor)
                equipSM.convertBtnClicked = false
                equipSM.convertAttempts   = 0
                equipSM.phase  = 2
                equipSM.nextAt = now + 100
            else
                -- Not on cursor — rescan bags
                bagsCache = getPSInBags()
                local converted = nil
                for _, item in ipairs(bagsCache) do
                    if not item.spent and not item.sealed
                       and item.bag == equipSM.best.bag
                       and item.slot == equipSM.best.slot then
                        converted = item; break
                    end
                end
                if not converted then
                    for _, item in ipairs(bagsCache) do
                        if not item.spent and not item.sealed and roleMatches(item.role, myRole) then
                            converted = item; break
                        end
                    end
                end
                if not converted then
                    printf('\ar[PS]\ax Auto-equip: could not find converted item — aborting.')
                    equipSM.phase           = 0
                    equipSM.best            = nil
                    equipSM.convertBtnClicked = false
                    equipSM.convertAttempts   = 0
                else
                    printf('\ay[PS]\ax Auto-equip: conversion done, picking up \ag%s\ax (T%d)',
                           converted.name, converted.tier)
                    equipSM.best            = converted
                    equipSM.convertBtnClicked = false
                    equipSM.convertAttempts   = 0
                    if converted.slot == 0 then
                        mq.cmdf('/itemnotify pack%d leftmouseup', converted.bag)
                    else
                        mq.cmdf('/itemnotify in pack%d %d leftmouseup', converted.bag, converted.slot)
                    end
                    equipSM.phase  = 2
                    equipSM.nextAt = now + 400
                end
            end
        end
    end

    -- ---- Auto-equip phase 2: equip cursor item → powersource slot ----
    if equipSM.phase == 2 and now >= equipSM.nextAt then
        -- Nil-safe Cursor read: mq.TLO.Cursor() returns item name string or nil
        local cursor = mq.TLO.Cursor and mq.TLO.Cursor()
        printf('\ay[PS]\ax Auto-equip phase 2: cursor after pickup = %s',
               tostring(cursor or 'empty'))
        if not cursor then
            printf('\ar[PS]\ax Auto-equip: pickup step failed — cursor is empty; aborting.')
            equipSM.phase           = 0
            equipSM.best            = nil
            equipSM.convertBtnClicked = false
            equipSM.convertAttempts   = 0
        else
            -- /ctrl bypasses the attune confirmation dialog for attunable PS items
            printf('\ay[PS]\ax Auto-equip: firing /ctrl /itemnotify powersource leftmouseup')
            mq.cmd('/ctrl /itemnotify powersource leftmouseup')
            equipSM.phase  = 3
            equipSM.nextAt = now + 400   -- wait for equip animation
        end
    end

    -- ---- Auto-equip phase 3: handle attune dialog, then check cursor ----
    if equipSM.phase == 3 and now >= equipSM.nextAt then
        -- Some items (e.g. Luclinite) show a confirmation dialog even with /ctrl.
        -- Click Yes and loop back for another tick before checking cursor state.
        local confWnd = mq.TLO.Window('ConfirmationDialogBox')
        if confWnd and confWnd.Open() then
            printf('\ay[PS]\ax Auto-equip phase 3: attune confirmation dialog — clicking Yes')
            local yesBtn = confWnd.Child('CD_Yes_Button')
            if yesBtn and yesBtn() then yesBtn.LeftMouseUp() end
            equipSM.nextAt = now + 400   -- wait for dialog to close & equip to register
        else
            -- No dialog (or already dismissed) — proceed with cursor / slot check
            local psNow  = mq.TLO.Me.Inventory('powersource')
            -- Nil-guard: psNow is an item object; psNow() checks slot validity;
            -- psNow.Name() is only called when the slot is populated.
            local psName = (psNow and psNow() and psNow.Name()) or 'empty'
            printf('\ay[PS]\ax Auto-equip phase 3: powersource slot = %s', psName)
            local cursor = mq.TLO.Cursor and mq.TLO.Cursor()
            if cursor then
                printf('\ay[PS]\ax Auto-equip: old PS on cursor (%s) → /autoinventory',
                       tostring(cursor))
                mq.cmd('/autoinventory')
                equipSM.phase  = 4
                equipSM.nextAt = now + 400   -- wait for bag placement
            else
                -- Nothing on cursor — equip went clean, no old PS to stow
                printf('\ay[PS]\ax Auto-equip: equip complete (no old PS on cursor)')
                equippedCache = getEquippedPS()
                bagsCache     = getPSInBags()
                if peers[myName] then
                    peers[myName].equipped = equippedCache
                    peers[myName].bags     = sanitizeBags(bagsCache)
                end
                equipSM.phase           = 0
                equipSM.best            = nil
                equipSM.convertBtnClicked = false
                equipSM.convertAttempts   = 0
            end
        end
    end

    -- ---- Auto-equip phase 4: post-autoinventory cache refresh ----
    if equipSM.phase == 4 and now >= equipSM.nextAt then
        local psNow  = mq.TLO.Me.Inventory('powersource')
        -- Nil-guard: same pattern as phase 2 — only dereference Name() when valid
        local psName = (psNow and psNow() and psNow.Name()) or 'empty'
        printf('\ay[PS]\ax Auto-equip complete: powersource = \ag%s\ax', psName)
        equippedCache = getEquippedPS()
        bagsCache     = getPSInBags()
        if peers[myName] then
            peers[myName].equipped = equippedCache
            peers[myName].bags     = sanitizeBags(bagsCache)
        end
        equipSM.phase           = 0
        equipSM.best            = nil
        equipSM.convertBtnClicked = false
        equipSM.convertAttempts   = 0
    end

    end)  -- end pcall wrapper for equipSM
    if not _equipOk then
        printf('\ar[PS]\ax equipSM crash (phase=%d): %s — resetting SM',
               equipSM.phase, tostring(_equipErr))
        equipSM.phase           = 0
        equipSM.best            = nil
        equipSM.convertBtnClicked = false
        equipSM.convertAttempts   = 0
        postTradeEquipAt = 0
    end
end

-- ============================================================
-- ImGui colors (r, g, b, a as separate floats for MQ2 Lua)
-- ============================================================
local C_OK   = { 0.2, 0.9, 0.2, 1.0 }
local C_WARN = { 0.9, 0.9, 0.1, 1.0 }
local C_BAD  = { 0.9, 0.2, 0.2, 1.0 }
local C_DIM  = { 0.6, 0.6, 0.6, 1.0 }

local function textCol(c, text)
    imgui.TextColored(c[1], c[2], c[3], c[4], text)
end

local function entryColor(equipped, role)
    if not equipped                           then return C_BAD  end
    if equipped.spent                         then return C_BAD  end
    if equipped.role == 'unknown'             then return C_WARN end
    if not roleMatches(equipped.role, role)   then return C_WARN end
    return C_OK
end

local function entryStatus(equipped, role)
    if not equipped                           then return 'MISSING'    end
    if equipped.spent                         then return 'SPENT'      end
    if not roleMatches(equipped.role, role)   then return 'WRONG ROLE' end
    return 'OK'
end

local function powerStr(eq)
    if not eq then return '-' end
    if eq.power ~= nil then
        if eq.maxPower ~= nil and eq.maxPower > 0 then
            return math.floor(eq.power / eq.maxPower * 100) .. '%'
        end
        return tostring(eq.power)
    end
    if eq.charges == -1 then return 'perm' end
    return tostring(eq.charges)
end

-- ============================================================
-- ImGui render callback
-- ============================================================
local function renderUI()
    if not showUI then return end

    imgui.SetNextWindowSize(880, 290, ImGuiCond.FirstUseEver)
    local _psWinFlags = ImGuiWindowFlags.HorizontalScrollbar
    -- imgui.Begin returns (pOpen, shouldDraw): update showUI from the X-button state,
    -- check shouldDraw so we skip rendering (and avoid style-stack corruption) when collapsed.
    local open
    showUI, open = imgui.Begin('Power Source Tracker##ps_tracker', showUI, _psWinFlags)
    if not open then
        imgui.End()
        return
    end

    if imgui.SmallButton('Hide##ps') then showUI = false end
    imgui.SameLine()

    if imgui.SmallButton('Start All##ps') then
        dexAll('/lua run ps_tracker')
    end
    imgui.SameLine()
    -- Use /lua stop (not /ps stop) so the stop doesn't depend on the
    -- /ps bind existing on the remote toon, and MQ cleans up the script
    -- object before we ask it to run again.
    if imgui.SmallButton('Stop All##ps') then
        dexAll('/lua stop ps_tracker')
    end
    imgui.SameLine()
    -- Restart All: stop peers now, then start them 1.5 s later via the
    -- main loop so the script has time to finish its cleanup before the
    -- new instance is requested.  mq.delay() must NOT be called here
    -- (ImGui callbacks are non-yieldable).
    if imgui.SmallButton('Restart All##ps') then
        dexAll('/lua stop ps_tracker')
        pendingRestartAt = mq.gettime() + 1500
    end
    imgui.SameLine()
    textCol(C_WARN, 'Power Source Tracker')

    -- Build display list: self first, then peers sorted by name
    local list = {}
    local selfEntry = peers[myName]
    if selfEntry then table.insert(list, selfEntry) end
    local peerNames = {}
    for n in pairs(peers) do
        if n ~= myName then table.insert(peerNames, n) end
    end
    table.sort(peerNames)
    for _, n in ipairs(peerNames) do table.insert(list, peers[n]) end

    local tflags = ImGuiTableFlags.Borders
                 + ImGuiTableFlags.RowBg
                 + ImGuiTableFlags.ScrollY
                 + ImGuiTableFlags.SizingStretchProp

    if imgui.BeginTable('##ps_tbl', 6, tflags) then
        imgui.TableSetupScrollFreeze(0, 1)
        imgui.TableSetupColumn('Name',           ImGuiTableColumnFlags.WidthFixed,   95)
        imgui.TableSetupColumn('Class',          ImGuiTableColumnFlags.WidthFixed,   40)
        imgui.TableSetupColumn('Power Source',   ImGuiTableColumnFlags.WidthStretch)
        imgui.TableSetupColumn('Tier',           ImGuiTableColumnFlags.WidthFixed,   32)
        imgui.TableSetupColumn('Power',          ImGuiTableColumnFlags.WidthFixed,   52)
        imgui.TableSetupColumn('Status / Notes', ImGuiTableColumnFlags.WidthStretch)
        imgui.TableHeadersRow()

        local now = os.time()
        for _, entry in ipairs(list) do
            if not entry or not entry.name then goto continue end
            local stale = (now - (entry.ts or now)) > PEER_TIMEOUT_S
            imgui.TableNextRow()

            imgui.TableSetColumnIndex(0)
            if stale then
                textCol(C_DIM, entry.name .. '*')
            else
                imgui.Text(entry.name)
            end

            imgui.TableSetColumnIndex(1)
            imgui.Text(entry.class or '?')

            imgui.TableSetColumnIndex(2)
            local eq  = entry.equipped
            local col = entryColor(eq, entry.role or 'melee')
            textCol(col, eq and eq.name or 'None')

            imgui.TableSetColumnIndex(3)
            imgui.Text(eq and tostring(eq.tier) or '-')

            imgui.TableSetColumnIndex(4)
            local ps = powerStr(eq)
            if eq and eq.spent then
                textCol(C_BAD, ps)
            else
                imgui.Text(ps)
            end

            imgui.TableSetColumnIndex(5)
            local status = entryStatus(eq, entry.role or 'melee')
            textCol(col, status)
            if status == 'WRONG ROLE' then
                local wSrc = findTradeSource(entry.role or 'melee')
                if imgui.IsItemHovered() then
                    imgui.BeginTooltip()

                    -- Swap path
                    imgui.TextColored(1, 1, 0, 1, '[Swap]')
                    imgui.SameLine()
                    -- Check if the WRONG ROLE toon has a matching PS in their own bags
                    -- (strict: role or 'all' only).  Use bagsCache if it's the local toon,
                    -- otherwise look in peers[entry.name].bags.
                    local localPS = nil
                    local bagSrc = (entry.name == myName)
                                   and bagsCache
                                   or (peers[entry.name] and peers[entry.name].bags or {})
                    for _, item in ipairs(bagSrc) do
                        if not item.spent and not item.sealed
                           and (item.role == (entry.role or 'melee') or item.role == 'all') then
                            localPS = item
                            break
                        end
                    end
                    if localPS then
                        imgui.Text(('Equip %s (T%d) from own bags'):format(localPS.name, localPS.tier))
                    elseif wSrc then
                        local shortName = wSrc.item.name:gsub('^Sealed ', ''):gsub(' Source$', '')
                        local sealNote = wSrc.item.sealed and ' → convert → equip' or ' → equip'
                        if wSrc.peer == myName then
                            imgui.Text(('Equip %s T%d from own bags%s'):format(shortName, wSrc.item.tier, sealNote))
                        else
                            imgui.Text(('%s trades %s T%d to %s%s'):format(wSrc.peer, shortName, wSrc.item.tier, entry.name, sealNote))
                        end
                    else
                        imgui.TextDisabled('No suitable PS found in any toon\'s bags')
                    end

                    imgui.Separator()

                    -- Trade path
                    imgui.TextColored(1, 1, 0, 1, '[Trade]')
                    imgui.SameLine()
                    if wSrc then
                        local shortName = wSrc.item.name:gsub('^Sealed ', ''):gsub(' Source$', '')
                        local sealNote = wSrc.item.sealed and ' → %s converts → equips' or ' → %s equips'
                        sealNote = sealNote:format(entry.name)
                        if wSrc.peer == myName then
                            imgui.Text(('Trade %s T%d from own bags to %s%s'):format(shortName, wSrc.item.tier, entry.name, sealNote))
                        else
                            imgui.Text(('%s navs to %s → trades %s T%d%s'):format(wSrc.peer, entry.name, shortName, wSrc.item.tier, sealNote))
                        end
                    else
                        imgui.TextDisabled('No PS available to trade')
                    end

                    imgui.EndTooltip()
                end
                imgui.SameLine()
                if imgui.SmallButton(('Swap##sw_%s'):format(entry.name)) then
                    if entry.name == myName then
                        -- local toon: set deferred flag (cannot call mq.delay from ImGui)
                        pendingForceSwap = true
                    elseif mailbox then
                        -- remote toon: send actor message
                        mailbox:send(
                            {character=entry.name, script='ps_tracker', mailbox='ps_tracker'},
                            {type='do_swap'}
                        )
                    end
                end
            end
            local needsSource = not eq or eq.spent or (eq and not roleMatches(eq.role, entry.role or 'melee'))
            if needsSource then
                local src = findTradeSource(entry.role or 'melee')
                if src then
                    imgui.SameLine()
                    if imgui.SmallButton(('Trade##tr_%s'):format(entry.name)) then
                        if src.peer == myName then
                            -- Source is in our own bags — start tradeSM directly
                            tradeSM.active    = true
                            tradeSM.destName  = entry.name
                            tradeSM.itemName  = src.item.name
                            tradeSM.phase     = 'nav'
                            tradeSM.nextTick  = 0
                            tradeSM.openTries = 0
                            printf('\ay[PS]\ax Trade (local): giving \at%s\ax to \at%s',
                                src.item.name, entry.name)
                        elseif mailbox then
                            -- Source is on a peer — send actor message
                            -- 'character' targets a specific EQ toon; 'name' is for
                            -- standalone actor clients (external apps) — wrong field.
                            mailbox:send(
                                { character=src.peer, script='ps_tracker', mailbox='ps_tracker' },
                                { type='do_trade', dest=entry.name, item=src.item.name }
                            )
                            printf('\ay[PS]\ax Sent trade request to \at%s\ax: give %s to %s',
                                src.peer, src.item.name, entry.name)
                        end
                    end
                end
            end

            ::continue::
        end

        imgui.EndTable()
    end

    imgui.Separator()
    local scanAge = math.max(0, math.floor((mq.gettime() - lastScan) / 1000))
    local nPeers  = 0; for _ in pairs(peers) do nPeers = nPeers + 1 end
    imgui.TextDisabled(('Scan: %ds ago  |  Toons: %d  |  Mode: %s'):format(
        scanAge, nPeers, mailbox and 'DanNet' or 'solo'))

    imgui.End()
end

-- ============================================================
-- Slash commands
-- ============================================================
mq.bind('/ps', function(arg)
    arg = ((arg or ''):lower():gsub('^%s+', ''):gsub('%s+$', ''))
    if arg == 'stop' then
        running = false
    elseif arg == 'ui' then
        showUI = not showUI
        printf('\ay[PS]\ax UI %s.', showUI and 'shown' or 'hidden')
    elseif arg == 'swap' then
        equippedCache = getEquippedPS()
        bagsCache     = getPSInBags()
        if not equippedCache then
            printf('\ay[PS]\ax No PS equipped.')
        elseif not equippedCache.spent then
            printf('\ay[PS]\ax %s is not spent — no swap needed.', equippedCache.name)
        else
            if trySwapPS() then
                equippedCache = getEquippedPS()
                bagsCache     = getPSInBags()
            end
        end
    elseif arg == 'scan' then
        equippedCache = getEquippedPS()
        bagsCache     = getPSInBags()
        peers[myName] = {
            name     = myName,
            class    = myClass,
            role     = myRole,
            equipped = equippedCache,
            bags     = sanitizeBags(bagsCache),
            ts       = os.time(),
        }
        broadcastStatus()
        local nSealed = 0
        for _, v in ipairs(bagsCache) do
            if v.sealed then nSealed = nSealed + 1 end
        end
        printf('\ay[PS]\ax Equipped: %s | Bag PS: %d (sealed: %d)',
               equippedCache and equippedCache.name or 'None', #bagsCache, nSealed)
    elseif arg == 'convert' then
        -- Refresh bag cache so we have accurate bag/slot coords
        bagsCache = getPSInBags()
        local sealed = nil
        for _, item in ipairs(bagsCache) do
            if item.sealed then
                sealed = item
                break
            end
        end
        if not sealed then
            printf('\ay[PS]\ax No sealed PS found in bags to convert.')
        else
            printf('\ay[PS]\ax Converting \ag%s\ax (pack%d slot %d) — right-clicking...',
                   sealed.name, sealed.bag, sealed.slot)
            mq.cmdf('/itemnotify in pack%d %d rightmouseup', sealed.bag, sealed.slot)
        end
    else
        printf('\ay[PS]\ax /ps stop | ui | swap | scan | convert')
    end
end)

-- ============================================================
-- Trade state machine — called each main loop tick (non-blocking)
-- ============================================================
local function tradeStep()
    if not tradeSM.active then return end
    local now = mq.gettime()
    if now < tradeSM.nextTick then return end

    if tradeSM.phase == 'nav' then
        if (mq.TLO.Target.Name() or '') ~= tradeSM.destName then
            printf('\ay[PS]\ax [trade:nav] targeting %s', tradeSM.destName)
            mq.cmdf('/target %s', tradeSM.destName)
            tradeSM.nextTick = now + 600
            return
        end
        local dist = mq.TLO.Target.Distance() or 999
        if dist > 12 then
            -- Guard against MQ2Nav not being loaded: Navigation TLO is nil when
            -- the plugin is absent, and indexing nil throws a Lua error.
            if mq.TLO.Navigation then
                if not mq.TLO.Navigation.Active() then
                    printf('\ay[PS]\ax [trade:nav] /nav target (dist=%.1f)', dist)
                    mq.cmd('/nav target')
                end
            else
                -- MQ2Nav not loaded — fall back to /moveto
                local tid = mq.TLO.Target.ID()
                if tid and tid > 0 then
                    printf('\ay[PS]\ax [trade:nav] MQ2Nav absent, /moveto id %d (dist=%.1f)', tid, dist)
                    mq.cmdf('/moveto id %d', tid)
                end
            end
            tradeSM.nextTick = now + 1000
        else
            if mq.TLO.Navigation then mq.cmd('/nav stop') end
            printf('\ay[PS]\ax [trade:nav] reached %s (dist=%.1f) → open phase', tradeSM.destName, dist)
            tradeSM.phase    = 'open'
            tradeSM.openTries = 0
            tradeSM.nextTick = now + 400
        end

    elseif tradeSM.phase == 'open' then
        -- In EQ, /click left target on a PC only opens the trade window when you
        -- have an item on the cursor.  The mechanic is:
        --   1. Pick up the item via /shift /itemnotify → item lands on cursor
        --   2. /click left target → EQ opens TradeWnd and places cursor item in slot 0
        -- (Confirmed in EZInventory inventory_actor.lua lines 1427-1439.)
        --
        -- Debug: show current state every pass
        -- Nil-guard the window object before calling .Open() on it
        local _twOpen   = mq.TLO.Window('TradeWnd')
        local tradeOpen  = _twOpen and _twOpen.Open()
        local cursorItem = mq.TLO.Cursor()
        printf('\ay[PS]\ax [trade:open] try=%d TradeWnd=%s cursor=%s',
            tradeSM.openTries, tostring(tradeOpen), tostring(cursorItem or 'empty'))

        if tradeOpen then
            printf('\ay[PS]\ax [trade:open] TradeWnd open → confirm phase (item placed via cursor-click)')
            tradeSM.phase    = 'confirm'
            tradeSM.nextTick = now + 300
            return
        end

        if tradeSM.openTries >= 5 then
            -- Return cursor item to bags if we bailed out
            if mq.TLO.Cursor() then
                printf('\ay[PS]\ax [trade:open] returning cursor item to bags')
                mq.cmd('/autoinventory')
                mq.delay('300ms')
            end
            printf('\ar[PS]\ax Could not open trade with %s after %d tries — aborting.', tradeSM.destName, tradeSM.openTries)
            tradeSM.active = false
            tradeSM.phase  = 'idle'
            return
        end

        if not mq.TLO.Cursor() then
            -- Step 1: find the item and pick it up onto the cursor
            local found = false
            for bag = 1, 10 do
                local pack = mq.TLO.Me.Inventory(('pack%d'):format(bag))
                if pack and pack() and (pack.Container() or 0) > 0 then
                    for slot = 1, pack.Container() do
                        local it = pack.Item(slot)
                        if it and it() and it.Name() == tradeSM.itemName then
                            printf('\ay[PS]\ax [trade:open] picking up "%s" from pack%d slot %d',
                                tradeSM.itemName, bag, slot)
                            mq.cmdf('/shift /itemnotify in pack%d %d leftmouseup', bag, slot)
                            found = true
                            break
                        end
                    end
                end
                if found then break end
            end
            if not found then
                printf('\ar[PS]\ax [trade:open] item "%s" not found in bags — aborting.', tradeSM.itemName)
                tradeSM.active = false
                tradeSM.phase  = 'idle'
            else
                tradeSM.nextTick = now + 500   -- wait for pick-up animation
            end
        else
            -- Step 2: cursor has the item — click target to open trade window
            -- This places the cursor item into trade slot 0 and opens TradeWnd
            printf('\ay[PS]\ax [trade:open] cursor has "%s", left-clicking target (attempt %d)',
                tostring(mq.TLO.Cursor()), tradeSM.openTries + 1)
            mq.cmd('/click left target')
            tradeSM.openTries = tradeSM.openTries + 1
            tradeSM.nextTick  = now + 800
        end

    elseif tradeSM.phase == 'confirm' then
        local _twConfirm = mq.TLO.Window('TradeWnd')
        if _twConfirm and _twConfirm.Open() then
            printf('\ay[PS]\ax [trade:confirm] clicking Trade button')
            local tradeBtn = _twConfirm.Child('TRDW_Trade_Button')
            if not tradeBtn or not tradeBtn() then
                printf('\ar[PS]\ax [trade:confirm] TRDW_Trade_Button child not found — cannot click Trade.')
            else
                tradeBtn.LeftMouseUp()
                printf('\ag[PS]\ax Trade sent to %s — waiting for accept.', tradeSM.destName)
            end
        else
            printf('\ar[PS]\ax [trade:confirm] TradeWnd closed before confirm — trade may have failed.')
        end
        tradeSM.active = false
        tradeSM.phase  = 'idle'
    end
end

mq.imgui.init('PSTracker', renderUI)

-- ============================================================
-- Main loop
-- ============================================================
printf('\ay[PS]\ax Running for \at%s\ax (%s / %s). /ps stop to quit.',
       myName, myClass, myRole)

while running do
    local now = mq.gettime()

    if now - lastScan >= SCAN_MS then
        equippedCache = getEquippedPS()
        bagsCache     = getPSInBags()

        peers[myName] = {
            name     = myName,
            class    = myClass,
            role     = myRole,
            equipped = equippedCache,
            bags     = sanitizeBags(bagsCache),
            ts       = os.time(),
        }

        if pendingForceSwap then
            pendingForceSwap = false
            printf('\ay[PS]\ax Swap: running forced equip for \at%s\ax (role=%s)', myName, myRole)
            local swapResult = trySwapPS(true, true)
            if swapResult then
                equippedCache          = getEquippedPS()
                bagsCache              = getPSInBags()
                peers[myName].equipped = equippedCache
                peers[myName].bags     = sanitizeBags(bagsCache)
                printf('\ay[PS]\ax Swap: equip succeeded — cache refreshed.')
            else
                -- No suitable non-sealed PS in own bags — check for sealed or ask a peer
                local src = findTradeSource(myRole)
                if src then
                    if src.peer == myName then
                        -- Sealed PS is in our own bags — trigger convert path via equipSM
                        if src.item.sealed then
                            local packItem
                            if src.item.slot == 0 then
                                packItem = mq.TLO.Me.Inventory(('pack%d'):format(src.item.bag))
                            else
                                packItem = mq.TLO.Me.Inventory(('pack%d'):format(src.item.bag)).Item(src.item.slot)
                            end
                            if packItem and packItem() then
                                printf('\ay[PS]\ax Swap: converting sealed \ag%s\ax (T%d) in own bags',
                                       src.item.name, src.item.tier)
                                packItem.Inspect()
                                equipSM.best              = src.item
                                equipSM.phase             = 1
                                equipSM.nextAt            = now + 400
                                equipSM.convertBtnClicked = false
                                equipSM.convertAttempts   = 0
                            else
                                printf('\ar[PS]\ax Swap: found sealed item in own bags but TLO access failed — aborting.')
                            end
                        else
                            printf('\ay[PS]\ax Swap: found item in own bags but cannot equip it.')
                        end
                    elseif mailbox then
                        mailbox:send(
                            {character=src.peer, script='ps_tracker', mailbox='ps_tracker'},
                            {type='do_trade', dest=myName, item=src.item.name}
                        )
                        printf('\ay[PS]\ax No PS in bags — requesting trade from \at%s\ax (%s)',
                               src.peer, src.item.name)
                    else
                        printf('\ay[PS]\ax Swap: no mailbox available to request a trade.')
                    end
                else
                    printf('\ay[PS]\ax \arSwap: no %s PS found in any toon\'s bags.\ax', myRole)
                end
            end
        end

        if not equippedCache or equippedCache.spent then
            if trySwapPS() then
                equippedCache          = getEquippedPS()
                bagsCache              = getPSInBags()
                peers[myName].equipped = equippedCache
                peers[myName].bags     = sanitizeBags(bagsCache)
            end
        end

        lastScan = now
    end

    if now - lastBroadcast >= BCAST_MS then
        broadcastStatus()
        lastBroadcast = now
    end

    -- Deferred peer restart: fire after the stop has had time to land
    if pendingRestartAt > 0 and mq.gettime() >= pendingRestartAt then
        dexAll('/lua run ps_tracker')
        pendingRestartAt = 0
    end

    tradeStep()
    autoAcceptStep()
    mq.doevents()
    mq.delay('100ms')
end

printf('\ay[PS]\ax Stopped.')
