-- init.lua  (ps_tracker)
-- DanNet-aware Power Source tracker for all toons.
--
-- Features:
--   * Scans equipped PS and bags every 5s; broadcasts state to all toons via actors
--   * ImGui overlay: each toon's PS name, tier, power % (or charges), and status
--   * Auto-swaps a spent PS for the best fresh one in bags (role-matched, tier-sorted)
--   * Auto-converts sealed PS items via ItemDisplayWindow before equipping
--   * Auto-accepts incoming PS trades (announced by a known peer) and equips the received item
--   * Trade/Swap buttons in the UI to coordinate PS distribution across the group
--     (a trade from a peer is only ever requested by a user click, never automatically)
--   * Never destroys or deletes anything; the cursor is verified before every pickup,
--     equip and /autoinventory
--
-- Install:  copy the ps_tracker/ folder into MQ/lua/
-- Run on every toon:  /lua run ps_tracker
-- Commands:
--   /ps stop      - stop the script
--   /ps ui        - toggle the ImGui window
--   /ps swap      - manually trigger a PS swap now
--   /ps scan      - force immediate re-scan and broadcast
--   /ps convert   - convert the first sealed PS in bags (ItemDisplayWindow); not equipped right away

local mq     = require('mq')
local imgui  = require('ImGui')
local actors = require('actors')

-- ============================================================
-- Power Source tier / role data
-- Power sources are found in the bags by name pattern or by the game's own data
-- (Item.WornSlot('powersource')); this data only supplies tier and role.
-- tier : higher = better
-- role : melee | tank | caster | healer | all
-- sealed : true -> stackable Misc item that needs right-click convert
--                  before it can be equipped; not a valid swap candidate
--
-- Named sets are "<Set> <Role word> Source" (and "Sealed <Set> <Role word> Source").
-- A new expansion set only needs one line in PS_SETS (and a new role word in
-- PS_ROLE_WORDS if it uses one). Generic one-off sources are listed by full name in PS_DB.
-- ============================================================
local PS_SETS = {
    ["Riven Arcana"] = 11,   -- Shattering of Ro (current highest, level 129)
    ["Unparalleled"] = 10,   -- Laurion's Song (seasonal/quest)
    ["Emberquartz"]  = 9,    -- Laurion's Song (standard current)
    ["Luclinite"]    = 8,    -- Terror of Luclin
    ["Shadowstone"]  = 7,    -- Night of Shadows (names UNVERIFIED - confirm in-game; wrong names are simply never matched)
}
local PS_ROLE_WORDS = {
    Slayer='melee',    Attacker='melee',
    Protector='tank',  Defender='tank',
    Conjuror='caster', Convoker='caster',
    Wayfarer='healer', Mendicant='healer',
}

-- Explicit entries: generic sources with no set/role naming (tier 5 and below).
local PS_DB = {
    ["Xorbb's Caged Hatred"]          = { tier=5,  role='all'    },
    ["Thaell Ew Totem"]               = { tier=4,  role='all'    },
    ["Ocean Purification Stone"]      = { tier=4,  role='all'    },
    ["Geomantic Steelstone"]          = { tier=4,  role='all'    },
    ["Combine Power Shard"]           = { tier=4,  role='all'    },
    ["Cliff Stone"]                   = { tier=3,  role='all'    },
    ["Enchanted Dragon Scale"]        = { tier=3,  role='all'    },
    ["Scorpikis Worry Stone"]         = { tier=3,  role='all'    },
    ["Magma Stone"]                   = { tier=3,  role='all'    },
}

-- Named-set items are derived from their name on first lookup and cached, so every
-- PS_DB[name] check also works for them. Unknown names return nil (and are not cached).
setmetatable(PS_DB, {
    __index = function(t, name)
        if type(name) ~= 'string' or #name > 80 then return nil end
        local sealed = false
        local rest = name
        if rest:sub(1, 7) == 'Sealed ' then sealed = true; rest = rest:sub(8) end
        local set, word = rest:match('^(.-) (%a+) Source$')
        local tier, role = set and PS_SETS[set], word and PS_ROLE_WORDS[word]
        if not (tier and role) then return nil end
        local entry = { tier = tier, role = role, sealed = sealed or nil }
        rawset(t, name, entry)
        return entry
    end,
})

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
local meTLO         = mq.TLO.Me
local myName        = meTLO and meTLO.Name() or nil
local myClassTLO    = meTLO and meTLO.Class or nil
local myClass       = myClassTLO and myClassTLO.ShortName() or nil
local myServer      = mq.TLO.EverQuest.Server() or ""
if not myName or not myClass then
    printf("\ay[PS]\ax Character data unavailable (not in game?) - exiting.")
    mq.exit()
end
local myRole        = CLASS_ROLE[myClass] or 'melee'
local equippedCache = nil
local bagsCache     = {}
local lastScan      = 0
local lastBroadcast = 0
---@type Dropbox
local mailbox       = nil   -- set once the helpers below exist (actors.register)

local SCAN_MS        = 5000
local BCAST_MS       = 5000

-- Trade automation state machine
-- phases: idle / nav / open / confirm / wait
local tradeSM = {
    active       = false,
    destName     = nil,   -- toon to trade TO
    itemName     = nil,   -- item to give
    phase        = 'idle',
    nextTick     = 0,
    openTries    = 0,
    pickTries    = 0,
    navStartAt   = 0,
    navTries     = 0,
    navStarted   = false, -- THIS script issued a /nav; only then may it /nav stop
    ownsWnd      = false, -- TradeWnd was opened (or may have been opened) by this trade run
    announced    = false, -- expect_trade sent to the recipient (a cancel_trade must follow an abort)
    waitUntil    = 0,
    openWaits    = 0,     -- ticks spent waiting for the equip machine to free the cursor
    placed       = false, -- THIS run clicked the target with our item on the cursor
    clickedAt    = 0,     -- mq.gettime() of that click
    movetoStarted = false, -- THIS run issued /moveto (only then may it /moveto off)
    movetoAt     = 0,
    runId        = nil,   -- unique id of this run; carried in expect_trade / cancel_trade
    countBefore  = 0,     -- how many copies of the item were in the bags when the run started
    acked        = false, -- the recipient answered expect_trade with expect_ack for this run id
    ackUntil     = 0,
    pickIssued   = false, -- THIS run issued the pickup of the item (only then may it stow/trade the cursor item)
    pickedUp     = false, -- ... and the item was seen on the cursor afterwards (verified pickup)
    cancelUntil  = 0,     -- end of the return-and-stow phase ('cancel')
    cancelMinAt  = 0,
    cancelStows  = 0,
    closeTries   = 0,
    blankSince   = 0,     -- ms when the TradeWnd partner name was first seen blank (0 = not blank)
    ownedItemWnd = false, -- at abort time: this run owned the window AND had placed the item in it
    -- Single-item pickup from a stack (see stackPickStep). stage: 0 none, 1 ctrl-click issued
    -- (waiting for QuantityWnd), 2 slider set to 1 (waiting for the input to read 1),
    -- 3 Accept clicked (waiting for the cursor).
    stackStage   = 0,
    stackIssued  = false, -- THIS run opened (or may have opened) QuantityWnd: it must be closed on every exit
    stackUntil   = 0,
    stackTries   = 0,
    stackN       = 1,     -- stack count seen in the bag before the pickup
}

-- All timestamps (peer staleness and state-machine timers) use mq.gettime() (ms).
local PEER_TIMEOUT_MS  = 30000
local NAV_TIMEOUT_MS   = 45000
local NAV_MAX_TRIES    = 30
local PEER_PRUNE_MS    = 300000  -- peers silent this long are dropped from the table
local PLACE_WAIT_MS    = 3000    -- after the click that places the item, do not re-pick for this long
local MOVETO_GAP_MS    = 8000    -- minimum gap before /moveto is issued again
local TRADE_STUCK_MS   = 30000   -- an accepted trade window that never closes is cancelled after this
local POST_TRADE_RETRIES = 5     -- bounded reschedules of the post-trade equip
local RESTART_WAIT_MS  = 25000   -- how long Restart All waits for a peer to be confirmed stopped
local RESTART_SILENT_MS = 3 * BCAST_MS  -- legacy peers only: this long without any status after /ps stop (3 broadcast intervals)
local TRADE_ACK_MS     = 3000    -- how long the giver waits for the recipient's expect_ack before opening a window
local TRADE_CANCEL_MS  = 2000    -- bounded return-and-stow phase after an aborted trade run
local VERIFY_WAIT_MS   = 2500    -- window in which a closed trade must keep showing the item as gone
local DO_SWAP_GAP_MS   = 5000    -- minimum gap between accepted remote swap requests
local NOPS_WARN_GAP_MS = 120000
local TRADE_WAIT_MS    = 20000   -- how long we wait for the recipient to accept
local DO_TRADE_GAP_MS  = 10000   -- minimum gap between accepted do_trade requests
local SWAP_RETRY_MS    = 30000   -- gap between failed automatic swap attempts
local SWAP_MAX_FAILS   = 3       -- automatic swap/convert attempts that may fail before pausing
local STACK_WND_MS     = 2500    -- stack pick: wait for QuantityWnd after the ctrl-click / for the slider text
local STACK_PICK_MS    = 2500    -- stack pick: wait for the item on the cursor after Accept
-- Kill switch for trading ONE item out of a stack (Ctrl-click single pickup confirmed in game;
-- only the QuantityWnd fallback is untested, see stackPickStep).
-- false: stacks larger than 1 are never offered or traded, as before.
local STACK_TRADE_ENABLED = true

-- An incoming trade is only auto-accepted when a peer announced it. Announcements are kept per
-- giver (key = giver name), each: { from=name, item=name, at=ms, id=run id, needFresh=bool }
-- The item name is only what the peer announced: no TLO exposes the contents of the
-- trade window slots, so it cannot be verified against what actually arrives.
-- needFresh: the TradeWnd was already open when the announcement arrived, so this announcement
-- may only authorize a window that opens later (the flag is cleared, and the announcement kept
-- with its TTL, once the open window closes).
local pendingIncoming = {}
-- Per-peer message bookkeeping used by Restart All (see restartStep). lastHeard[name] is the
-- time of the last valid status broadcast from that peer; lastBye[name] the last 'bye'.
local lastHeard = {}
local lastBye   = {}
local tradeRunCounter = 0   -- makes each giver run id unique within this script run
local lastDoSwapAt         = 0
local lastDoTradeAt        = 0
local lastNoPSWarnAt       = 0
local lastPeerHintAt       = 0
local lastConvertFailAt    = 0
local autoEquipPausedUntil = 0     -- a manual /ps convert pauses the automatic equip briefly
local swapping             = false -- trySwapPS (which uses mq.delay) is running: nothing else may start
local lastAutoSwapAt       = 0
local autoSwapFails        = 0
local CONVERT_RETRY_MS     = 60000

local PENDING_TRADE_TTL_MS = 60000

local function numBags() return mq.TLO.Me.NumBagSlots() or 10 end

-- Throttled printf for messages that can repeat from a periodic path (one line per key per gap).
local logAt = {}
local function logThrottled(key, gapMs, fmt, ...)
    local now = mq.gettime()
    if logAt[key] and now - logAt[key] < gapMs then return end
    logAt[key] = now
    printf(fmt, ...)
end

-- Text of a child control of a window, or nil. Window text members can be missing on a child that
-- does not exist in the current UI skin (calling one raised "attempt to call field 'Text' (a nil
-- value)" and killed the script), so every read is guarded and returns nil instead of erroring.
local function ctlText(win, name)
    if not win then return nil end
    local ok, txt = pcall(function()
        local ctl = win.Child(name)
        if not (ctl and ctl()) then return nil end
        local t = ctl.Text
        if not t then return nil end
        return t()
    end)
    if ok and type(txt) == 'string' then return txt end
    return nil
end

-- A sealed convert failed recently: do not reopen ItemDisplayWindow every scan.
local function convertRecentlyFailed()
    return lastConvertFailAt > 0 and (mq.gettime() - lastConvertFailAt) < CONVERT_RETRY_MS
end

-- Is the generic confirmation dialog open right now? Callers record this BEFORE their own
-- pickup/equip click, so clickAttuneConfirm can refuse a dialog that was already up.
local function confirmDialogOpen()
    local wnd = mq.TLO.Window('ConfirmationDialogBox')
    return (wnd and wnd.Open()) and true or false
end

-- Click Yes on the confirmation dialog only when it is about attuning/binding the item.
-- wasOpenBefore: result of confirmDialogOpen() taken before the operation's own click; the dialog
-- must have appeared AFTER that click (false), otherwise (true or nil = unknown) nothing is clicked.
-- ...: names of the item(s) this operation handles. The dialog text (CD_TextOutput, content
-- UNVERIFIED) must not name a different Power Source: a text that mentions "source" but none of
-- these names is refused.
local function clickAttuneConfirm(wasOpenBefore, ...)
    if wasOpenBefore ~= false then return false end
    local wnd = mq.TLO.Window('ConfirmationDialogBox')
    if not wnd or not wnd.Open() then return false end
    local txt = (ctlText(wnd, 'CD_TextOutput') or ''):lower()
    if not (txt:find('attune', 1, true) or txt:find('bound', 1, true) or txt:find('bind', 1, true)) then
        return false
    end
    if txt:find('%f[%a]source%f[%A]') then
        local named = false
        for _, nm in ipairs({ ... }) do
            if type(nm) == 'string' and nm ~= '' and txt:find(nm:lower(), 1, true) then named = true end
        end
        if not named then
            logThrottled('attune_other', 10000, "\ar[PS]\ax Confirmation dialog seems to be about another item - not clicking Yes.")
            return false
        end
    end
    local yesBtn = wnd.Child('CD_Yes_Button')
    if yesBtn and yesBtn() then
        printf("\ay[PS]\ax Attune confirmation dialog - clicking Yes")
        yesBtn.LeftMouseUp()
        return true
    end
    return false
end

-- Auto-accept / auto-equip state (incoming trade)
local tradeRecvAt      = 0      -- mq.gettime() when we first detected ourselves as recipient
local tradeAccepted    = false  -- true only after we found AND clicked the Trade button; cleared when TradeWnd closes
local tradeAcceptedAt  = 0      -- mq.gettime() of that click
local tradeWasOpen     = false  -- TradeWnd was open (not started by our giver run); used on the close edge
local tradeWndPartner  = ''     -- partner name last read from that window ('' = not read yet)
local tradeAcceptedPartner = '' -- partner of the incoming window WE accepted (shutdown cancels only that one)
local tradeRejectLogged = false -- an ignored window: logged once AND latched (never authorized) until it closes
local tradeWndOpenAt   = 0      -- mq.gettime() when the current TradeWnd open edge was first seen (0 = closed)
local postTradeEquipAt = 0      -- mq.gettime() after which we scan bags and auto-equip
local postTradeRetries = 0      -- bounded reschedules of the post-trade equip

-- Non-blocking equip state machine (phases driven by main loop)
-- phase 0=idle, 1=convert, 2=equip_pending, 3=equip_sent, 4=inventory_sent
--   expect  : exact item name that must be on the cursor before equipping (nil = any unsealed role match)
--   priorPS : name of the PS that was in the slot before the equip (it lands on the cursor)
local equipSM = {
    phase=0, best=nil, nextAt=0, convertBtnClicked=false, convertAttempts=0,
    expect=nil, priorPS=nil, attuneClicks=0,
    convertOnly=false,   -- true: stop after the convert (item stays unsealed in the bags)
    fromConvert=false,   -- the item being equipped came out of a sealed convert
    intended=nil,        -- name of the item phase 2 equipped; the slot must hold it afterwards
    before=nil,          -- keys of the unsealed PS items present before the Convert click
    expectName=nil,      -- name of the unsealed item the sealed one must turn into
    beforeCount=nil,     -- how many copies of expectName were in the bags + cursor before the click
    dlgBefore=nil,       -- ConfirmationDialogBox was open before our own click (nil = unknown: never click Yes)
}

-- A failed sealed convert: remember when (pauses sealed retries) and count it toward the
-- automatic failure cap so the script pauses instead of retrying forever.
-- A manual /ps convert (convertOnly) does not count.
local function noteConvertFail(now)
    lastConvertFailAt = now
    if equipSM.convertOnly then return end
    autoSwapFails = autoSwapFails + 1
    if autoSwapFails >= SWAP_MAX_FAILS then
        printf("\ar[PS]\ax Sealed convert failed %d times - pausing automatic attempts. Use /ps swap to retry.",
               autoSwapFails)
    end
end

-- ============================================================
-- Utility
-- ============================================================
local function psTier(name)   return PS_DB[name] and PS_DB[name].tier   or 0     end
local function psRole(name)   return PS_DB[name] and PS_DB[name].role            end
local function psSealed(name) return PS_DB[name] and PS_DB[name].sealed or false end

-- Detect a power source by the game's own data (Item.WornSlot('powersource')) instead of a
-- name list. PS_DB then only supplies tier and role. A PS the game recognizes but PS_DB lacks
-- cannot be ranked or role-matched, so it is skipped and reported once so it can be added.
-- (Sealed PS items are not wearable, so they are still recognized by PS_DB name only.)
local warnedUnknownPS = {}
local function isPSItem(item, name)
    if not name or name == '' then return false end
    if PS_DB[name] then return true end
    if warnedUnknownPS[name] == nil then
        -- Cache the answer per name (true = warned, false = not a PS) so ordinary bag items
        -- are only probed once, not on every scan.
        local ok, worn = pcall(function() return item.WornSlot('powersource')() end)
        warnedUnknownPS[name] = (ok and worn) and true or false
        if warnedUnknownPS[name] then
            printf('\ay[PS]\ax Power source "%s" is not in PS_DB (tier/role unknown) - skipped. Please report it.', name)
        end
    end
    return false
end

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
-- Cursor and bag safety helpers.
-- This script NEVER destroys or deletes anything. Every cursor action is
-- preceded by a check of what is actually on the cursor.
-- ============================================================

-- Name of the item on the cursor, or nil when the cursor is empty.
local function cursorName()
    local n = mq.TLO.Cursor.Name()
    if n == nil or n == '' then return nil end
    return n
end

-- True when the cursor is empty (required before any pickup). Logs otherwise.
local function cursorIsEmpty(context)
    local held = cursorName()
    if held then
        printf('\ar[PS]\ax [%s] cursor already holds "%s" - not touching it.', context, held)
        return false
    end
    return true
end

-- Put the cursor item away with /autoinventory, but only when it is plausibly ours:
-- a PS_DB item, or the item the caller just picked up / swapped out (`expected`).
-- Returns true when the cursor was empty or /autoinventory was issued; false when the
-- cursor holds something unexpected (left untouched).
local function safeAutoInventory(context, expected)
    local held = cursorName()
    if not held then
        printf("\ay[PS]\ax [%s] cursor is empty - nothing to put away", context)
        return true
    end
    printf('\ay[PS]\ax [%s] cursor holds "%s"', context, held)
    if PS_DB[held] == nil and held ~= expected then
        printf('\ar[PS]\ax [%s] "%s" is not one of our items - leaving it alone.', context, held)
        return false
    end
    mq.cmd("/autoinventory")
    return true
end

-- Peer names end up inside /target and /nav command strings: accept letters only.
local function validName(n)
    return type(n) == 'string' and #n <= 40 and n:match('^%a[%a]*$') ~= nil
end

-- True when a window is open that owns the cursor or the bags (trade, merchant, bank, loot).
-- No automatic pickup/equip may run while one of them is up.
-- ignoreTrade: the caller is the trade run itself, for which an open TradeWnd is expected.
local function uiBusy(ignoreTrade)
    for _, w in ipairs({ 'TradeWnd', 'MerchantWnd', 'BigBankWnd', 'LootWnd' }) do
        if not (ignoreTrade and w == 'TradeWnd') then
            local wnd = mq.TLO.Window(w)
            if wnd and wnd.Open() then return true end
        end
    end
    return false
end

-- Anything that owns the cursor or the bags: an equip step, a trade run, or trySwapPS.
local function opBusy()
    return equipSM.phase ~= 0 or tradeSM.active or swapping
end

-- Item TLO for a bag position (slot 0 = item sitting directly in the pack slot), or nil.
local function bagItem(bag, slot)
    local pack = mq.TLO.Me.Inventory(('pack%d'):format(bag))
    if not pack or not pack() then return nil end
    if slot == 0 then return pack end
    local it = pack.Item(slot)
    if not it or not it() then return nil end
    return it
end

-- True when the bag position still holds `name` (and, for non-sealed PS, it is not spent).
local function bagItemIs(bag, slot, name)
    local it = bagItem(bag, slot)
    if not it or it.Name() ~= name then return false end
    return psSealed(name) or not isItemSpent(it)
end

local function clickBagSlot(bag, slot)
    if slot == 0 then
        mq.cmdf("/itemnotify pack%d leftmouseup", bag)
    else
        mq.cmdf("/itemnotify in pack%d %d leftmouseup", bag, slot)
    end
end

-- Can this item be given away? It must not be NO TRADE. A single item is picked up with
-- /ctrl /itemnotify; a stack larger than 1 is only offered when STACK_TRADE_ENABLED, and then
-- exactly ONE item is split off through QuantityWnd (stackPickStep) and verified (Cursor.Stack == 1)
-- before it may be placed in the trade window.
local function tradeable(it)
    if it.NoDrop() == true then return false end   -- Item.NoDrop = "No Trade?" (definitions _item.lua)
    return STACK_TRADE_ENABLED or (it.Stack() or 1) <= 1
end

-- First bag position holding a usable (non-spent) copy of `name`: returns bag, slot or nil.
-- forTrade: also skip NO TRADE items (and stacks when STACK_TRADE_ENABLED is off), see tradeable.
local function findInBags(name, forTrade)
    local function usable(it)
        if not psSealed(name) and isItemSpent(it) then return false end
        return not forTrade or tradeable(it)
    end
    for bag = 1, numBags() do
        local pack = mq.TLO.Me.Inventory(('pack%d'):format(bag))
        if pack and pack() then
            if pack.Name() == name and usable(pack) then
                return bag, 0
            end
            for slot = 1, (pack.Container() or 0) do
                local it = pack.Item(slot)
                if it and it() and it.Name() == name and usable(it) then
                    return bag, slot
                end
            end
        end
    end
    return nil
end

-- How many items named `name` are in the bags (any state; a stack counts every item in it,
-- so giving away one item out of a stack lowers the count by exactly one).
local function countInBags(name)
    local n = 0
    for bag = 1, numBags() do
        local pack = mq.TLO.Me.Inventory(('pack%d'):format(bag))
        if pack and pack() then
            if pack.Name() == name then n = n + (pack.Stack() or 1) end
            for slot = 1, (pack.Container() or 0) do
                local it = pack.Item(slot)
                if it and it() and it.Name() == name then n = n + (it.Stack() or 1) end
            end
        end
    end
    return n
end

-- Name of the unsealed item a sealed PS turns into ("Sealed X Source" -> "X Source").
local function unsealedNameOf(name)
    return (name:gsub('^Sealed ', ''))
end

-- A peer is "known" when it has reported recently and is not us.
local function isKnownPeer(name)
    if not validName(name) or name == myName then return false end
    local p = peers[name]
    return p ~= nil and (mq.gettime() - (p.ts or 0)) <= PEER_TIMEOUT_MS
end

-- Same test the UI uses to decide whether a toon needs a PS from someone else.
local function peerNeedsPS(p)
    local eq = p.equipped
    return not eq or eq.spent or not roleMatches(eq.role, p.role or 'melee')
end

-- Rebuild a peer-supplied PS record from scratch: tier/role/sealed come from our own
-- PS_DB, numeric fields are type-checked, so a malformed message cannot break the UI.
local function cleanPS(t)
    if type(t) ~= 'table' or type(t.name) ~= 'string' or #t.name > 80 then return nil end
    local function num(v) return type(v) == 'number' and v or nil end
    return {
        name     = t.name,
        tier     = psTier(t.name),
        role     = psRole(t.name) or 'unknown',
        power    = num(t.power),
        maxPower = num(t.maxPower),
        charges  = num(t.charges) or -1,
        spent    = t.spent == true,
        sealed   = psSealed(t.name),
        noTrade  = t.noTrade == true,
        stack    = num(t.stack) or 1,
    }
end

-- server: the sender's server as reported by the actor layer (verified by vetSender), not the payload.
local function cleanPeerStatus(d, server)
    if not validName(d.name) then return nil end
    local bags = {}
    if type(d.bags) == 'table' then
        for i, it in ipairs(d.bags) do
            if i > 200 then break end
            local c = cleanPS(it)
            if c and PS_DB[c.name] then table.insert(bags, c) end
        end
    end
    return {
        name     = d.name,
        server   = server,
        class    = (type(d.class) == 'string' and #d.class <= 8) and d.class or '?',
        role     = (type(d.role) == 'string' and ROLE_FALLBACK[d.role]) and d.role or 'melee',
        equipped = cleanPS(d.equipped),
        bags     = bags,
        ts       = mq.gettime(),
    }
end

-- Reset the equip state machine. Closes ItemDisplayWindow if we opened it (phase 1).
local function resetEquipSM()
    if equipSM.phase == 1 then
        local idw = mq.TLO.Window('ItemDisplayWindow')
        if idw and idw.Open() then idw.DoClose() end
    end
    equipSM.phase             = 0
    equipSM.best              = nil
    equipSM.expect            = nil
    equipSM.priorPS           = nil
    equipSM.attuneClicks      = 0
    equipSM.convertBtnClicked = false
    equipSM.convertAttempts   = 0
    equipSM.convertOnly       = false
    equipSM.fromConvert       = false
    equipSM.intended          = nil
    equipSM.before            = nil
    equipSM.expectName        = nil
    equipSM.beforeCount       = nil
    equipSM.dlgBefore         = nil
end

-- Start a trade run; always resets the nav timeout and try counters.
-- Only a known peer may receive, and only a PS_DB item may be given.
local function startTradeSM(dest, item)
    if tradeSM.active then
        printf("\ay[PS]\ax A trade is already in progress - request ignored.")
        return false
    end
    if not isKnownPeer(dest) or PS_DB[item] == nil then
        printf("\ar[PS]\ax Trade refused: destination is not a known peer or the item is not a known Power Source.")
        return false
    end
    if equipSM.phase ~= 0 or swapping or uiBusy() then
        printf("\ay[PS]\ax Trade not started: busy (equip running, or a trade/merchant/bank/loot window is open).")
        return false
    end
    if not findInBags(item, true) then
        printf('\ay[PS]\ax Trade not started: no tradeable "%s" in my bags (it may be NO TRADE or spent).', item)
        return false
    end
    if not cursorIsEmpty('trade') then return false end
    tradeSM.active     = true
    tradeSM.destName   = dest
    tradeSM.itemName   = item
    tradeSM.phase      = 'nav'
    tradeSM.nextTick   = 0
    tradeSM.openTries  = 0
    tradeSM.pickTries  = 0
    tradeSM.navStartAt = mq.gettime()
    tradeSM.navTries   = 0
    tradeSM.navStarted = false
    tradeSM.ownsWnd    = false
    tradeSM.announced  = false
    tradeSM.waitUntil  = 0
    tradeSM.placed     = false
    tradeSM.openWaits  = 0
    tradeSM.clickedAt  = 0
    tradeSM.movetoStarted = false
    tradeSM.movetoAt   = 0
    tradeRunCounter    = tradeRunCounter + 1
    tradeSM.runId      = ('%s:%d:%d'):format(myName, mq.gettime(), tradeRunCounter)
    tradeSM.countBefore = countInBags(item)
    tradeSM.acked      = false
    tradeSM.ackUntil   = 0
    tradeSM.pickIssued = false
    tradeSM.pickedUp   = false
    tradeSM.stackStage  = 0
    tradeSM.stackIssued = false
    tradeSM.stackUntil  = 0
    tradeSM.stackTries  = 0
    tradeSM.stackN      = 1
    tradeSM.cancelUntil = 0
    tradeSM.cancelMinAt = 0
    tradeSM.cancelStows = 0
    tradeSM.closeTries  = 0
    tradeSM.blankSince  = 0
    tradeSM.ownedItemWnd = false
    return true
end

-- Open ItemDisplayWindow for a sealed PS and arm equipSM phase 1 to click Convert.
-- convertOnly: stop after the convert; the unsealed item stays in the bags (manual /ps convert).
local function startConvert(item, convertOnly)
    if opBusy() or uiBusy() then
        logThrottled('convert_busy', 10000, "\ay[PS]\ax Busy - convert skipped.")
        return false
    end
    local heldNow = cursorName()
    if heldNow then
        logThrottled('convert_cursor', 30000, '\ar[PS]\ax [convert] cursor already holds "%s" - not touching it.', heldNow)
        return false
    end
    -- Stacks: a stacked Sealed item is NOT refused here. Convert works on the item shown in
    -- ItemDisplayWindow (Inspect) and the result is verified by name/before-after keys, never by
    -- picking the stack up, so no cursor split is involved. Whether the Convert button consumes
    -- one item or the whole stack is UNVERIFIED; the result check still decides success.
    if not bagItemIs(item.bag, item.slot, item.name) then
        logThrottled('convert_gone', 10000, '\ar[PS]\ax Convert: "%s" is no longer at pack%d slot %d - aborting.',
               item.name, item.bag, item.slot)
        return false
    end
    -- An ItemDisplayWindow that is already open may be showing some other (sealed) item, and the
    -- Convert button would act on that one: only start when no such window is open. (The item the
    -- window shows is also compared with the intended one right before the Convert click.)
    local idw = mq.TLO.Window('ItemDisplayWindow')
    if idw and idw.Open() then
        logThrottled('convert_idw', 10000, "\ay[PS]\ax Convert skipped: close the open ItemDisplayWindow first.")
        return false
    end
    -- The result must be a known unsealed item (derived from the sealed name), or the convert
    -- could never be verified.
    local expectName = unsealedNameOf(item.name)
    if PS_DB[expectName] == nil or psSealed(expectName) then
        logThrottled('convert_noresult', 10000, '\ar[PS]\ax Convert skipped: "%s" does not map to a known unsealed Power Source.', item.name)
        return false
    end
    bagItem(item.bag, item.slot).Inspect()
    equipSM.expectName        = expectName
    equipSM.beforeCount       = nil
    equipSM.best              = item
    equipSM.expect            = nil
    equipSM.phase             = 1
    equipSM.nextAt            = mq.gettime() + 400  -- wait for ItemDisplayWindow to open
    equipSM.convertBtnClicked = false
    equipSM.convertAttempts   = 0
    equipSM.attuneClicks      = 0
    equipSM.convertOnly       = convertOnly == true
    equipSM.fromConvert       = true
    equipSM.before            = nil
    return true
end

-- Tell `dest` that we are about to trade it `item`, so it will auto-accept.
-- Sent by the giver itself, so the recipient can check the sender against `from`.
-- `id` is the giver's run id: a cancel only withdraws the announcement of that same run.
local function announceTrade(dest, item, id)
    mailbox:send(
        { character=dest, script='ps_tracker', mailbox='ps_tracker' },
        { type='expect_trade', from=myName, item=item, id=id, server=myServer }
    )
end

-- Withdraw an announcement (the giver aborted, timed out or the trade did not complete), so the
-- recipient stops expecting it. Scoped by run id so it cannot clear a newer announcement.
local function cancelTradeAnnouncement(dest, id)
    mailbox:send(
        { character=dest, script='ps_tracker', mailbox='ps_tracker' },
        { type='cancel_trade', from=myName, id=id, server=myServer }
    )
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
-- Returns a table sorted: fresh before spent; within each group by tier descending;
-- within the same tier unsealed before sealed.
-- ============================================================
local function bagEntry(it, name, bag, slot)
    local ch     = it.Charges()
    local sealed = psSealed(name)
    return {
        name     = name,
        bag      = bag,
        slot     = slot,   -- 0 = item sits directly in the top-level pack slot (not inside a bag)
        tier     = psTier(name),
        role     = psRole(name),
        power    = it.Power(),
        maxPower = it.MaxPower(),
        charges  = (ch ~= nil) and ch or -1,
        spent    = not sealed and isItemSpent(it),
        sealed   = sealed,
        noTrade  = it.NoDrop() == true,   -- Item.NoDrop = "No Trade?" (definitions _item.lua)
        stack    = it.Stack() or 1,
    }
end

local function getPSInBags()
    local found = {}
    for bag = 1, numBags() do
        local pack = mq.TLO.Me.Inventory(('pack%d'):format(bag))
        if pack and pack() then
            local topName = pack.Name()
            if topName and isPSItem(pack, topName) then
                table.insert(found, bagEntry(pack, topName, bag, 0))
            elseif (pack.Container() or 0) > 0 then
                for slot = 1, pack.Container() do
                    local item = pack.Item(slot)
                    if item and item() then
                        local name = item.Name()
                        if isPSItem(item, name) then
                            table.insert(found, bagEntry(item, name, bag, slot))
                        end
                    end
                end
            end
        end
    end

    -- Sort: fresh before spent; then highest tier first; within the same tier, unsealed before sealed.
    table.sort(found, function(a, b)
        if a.spent ~= b.spent then return not a.spent end
        if a.tier  ~= b.tier  then return a.tier > b.tier end
        if a.sealed ~= b.sealed then return not a.sealed end
        return false
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
            noTrade  = v.noTrade,
            stack    = v.stack,
        })
    end
    return out
end
-- ============================================================
-- Auto-swap / auto-equip: equip the best fresh PS for this toon.
-- Handles two cases:
--   A) Slot empty:
--      2-step equip - pick up from bag, equip directly (no old PS to stow).
--   B) Slot has a spent (or, when forced, a wrong-role) PS:
--      3-step swap - pick up the old PS from the slot, click bag slot of fresh PS
--      (EQ swaps them: old lands in bag, fresh on cursor), equip fresh.
-- Sealed items are SKIPPED - they cannot be equipped directly.
-- Every step verifies the cursor first; nothing unexpected is ever touched.
-- The equipped slot is re-read live before acting (the cached scan can be 5 s old), and the
-- result is verified afterwards.
-- This function blocks (mq.delay), so it is only called from the main loop and the
-- `swapping` flag keeps every other state machine and request handler out while it runs.
-- Returns: true on success; false plus an optional reason:
--   'sealed' (caller should convert), 'busy' (nothing sent), 'failed' (a step went wrong),
--   'fresh' (the slot already holds a suitable PS - nothing to do)
-- ============================================================

-- A fresh PS that fits this toon's role.
local function equippedSuits(eq)
    return eq ~= nil and not eq.spent and roleMatches(eq.role, myRole)
end

-- Wait (bounded) until the powersource slot holds `name`; true when it does.
local function waitSlotHolds(name)
    mq.delay('1500ms', function()
        local slot = mq.TLO.Me.Inventory('powersource')
        if not slot or not slot() then return false end
        return slot.Name() == name
    end)
    local eq = getEquippedPS()
    return eq ~= nil and eq.name == name
end

-- A merchant/bank/loot/trade window opened mid-run: put our item away and stop.
local function busyRecover(context, expected)
    if not uiBusy() then return false end
    printf("\ay[PS]\ax [%s] a trade/merchant/bank/loot window opened - stopping this equip.", context)
    if cursorName() then
        if safeAutoInventory(context, expected) then mq.delay('300ms') end
    end
    return true
end

-- Immediately before an equip click: is the powersource slot still in the state the decision
-- was based on? It must be empty, or (spentName given) hold that same spent item. A click on
-- any other occupied slot would swap somebody else's equipment onto the cursor.
local function slotStillExpected(spentName)
    local s = mq.TLO.Me.Inventory('powersource')
    if not s or not s() then return true end
    return spentName ~= nil and s.Name() == spentName and isItemSpent(s)
end

-- The slot changed under us: put the picked-up source away (cursor verified inside
-- safeAutoInventory) and leave whatever is worn now alone. Not counted as a failure ('busy').
local function equipRaceRecover(context, picked)
    printf('\ay[PS]\ax [%s] the powersource slot changed before the equip click - putting "%s" away, leaving the worn item alone.',
           context, picked)
    if safeAutoInventory(context, picked) then mq.delay('300ms') end
    return false, 'busy'
end

local function trySwapPSInner(force, strict)
    -- Another state machine owns the cursor, or a trade/merchant/bank/loot window is open
    if equipSM.phase ~= 0 or tradeSM.active or uiBusy() then return false, 'busy' end

    -- Needs action if forced, slot is empty, or equipped PS is spent
    local needsAction = force or (not equippedCache) or equippedCache.spent
    if not needsAction then return false end

    -- Live read: a suitable PS that is already worn is never swapped out.
    local live = getEquippedPS()
    if equippedSuits(live) then
        equippedCache = live
        return false, 'fresh'
    end

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
        -- Normal mode: before falling back to a wrong-role PS, check whether a
        -- sealed correct-role item exists that equipSM can convert.  Prefer
        -- converting the right item over equipping a wrong-role stopgap, but not when a
        -- fresh unsealed correct-role item of equal or higher tier is already available.
        -- Skipped while a convert recently failed so we do not stall on it.
        if not convertRecentlyFailed() then
            local sealedBest, unsealedBest
            for _, item in ipairs(bagsCache) do   -- sorted best-first
                if not item.spent and (item.role == myRole or item.role == 'all') then
                    if item.sealed then sealedBest = sealedBest or item
                    else unsealedBest = unsealedBest or item end
                end
            end
            if sealedBest and not (unsealedBest and unsealedBest.tier >= sealedBest.tier) then
                return false, 'sealed'   -- caller should trigger equipSM convert path
            end
        end
        -- Walk the full ROLE_FALLBACK chain.
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
        -- Throttled: this runs every scan while nothing is available.
        local nowMs = mq.gettime()
        if force or lastNoPSWarnAt == 0 or nowMs - lastNoPSWarnAt >= NOPS_WARN_GAP_MS then
            lastNoPSWarnAt = nowMs
            printf("\ay[PS]\ax \arAuto-equip: no fresh non-sealed PS found in bags.\ax")
        end
        return false
    end

    -- A fresh wrong-role PS is only replaced on request, and only by a correct-role PS.
    if live and not live.spent then
        if not force or not (best.role == myRole or best.role == 'all') then
            equippedCache = live
            return false, 'fresh'
        end
    end

    if usedFallback then
        printf("\ay[PS]\ax \ayNo %s PS available - equipping \at%s\ax (T%d, %s) as stopgap.\ax",
               myRole, best.name, best.tier, fallbackRole)
    end

    -- The scan is up to 5s old: make sure the bag slot still holds the item we chose.
    if not bagItemIs(best.bag, best.slot, best.name) then
        printf('\ar[PS]\ax Equip skipped: "%s" is no longer at pack%d slot %d (bags changed).',
               best.name, best.bag, best.slot)
        return false, 'busy'
    end

    if not live then
        -- Case A: slot is empty - 2-step equip
        printf("\ay[PS]\ax Equipping \ag%s\ax (T%d) from bags (slot was empty)",
               best.name, best.tier)

        if not cursorIsEmpty('equip') then return false, 'busy' end
        -- Re-check right before the click: something may have been equipped meanwhile
        if getEquippedPS() then
            printf("\ay[PS]\ax Equip skipped: the powersource slot is no longer empty.")
            return false, 'busy'
        end
        if uiBusy() then return false, 'busy' end
        local dlgBefore = confirmDialogOpen()   -- a dialog already open now is never ours
        clickBagSlot(best.bag, best.slot)
        mq.delay('300ms')
        local picked = cursorName()
        if picked ~= best.name then
            printf('\ar[PS]\ax Equip failed: expected "%s" on cursor, found "%s".',
                   best.name, tostring(picked or 'nothing'))
            if picked then safeAutoInventory('equip', best.name) end
            return false, 'failed'
        end
        if busyRecover('equip', best.name) then return false, 'busy' end
        if not slotStillExpected(nil) then return equipRaceRecover('equip', best.name) end

        mq.cmd("/ctrl /itemnotify powersource leftmouseup")
        mq.delay('200ms')
        -- Some items (e.g. Luclinite) still show an attune confirmation dialog
        -- even with /ctrl.  Click Yes if it appeared after our click (and is about attuning).
        if clickAttuneConfirm(dlgBefore, best.name) then
            mq.delay('300ms')
        end

        if cursorName() then
            printf("\ar[PS]\ax Equip anomaly: cursor still has an item after equip!")
            if safeAutoInventory('equip', best.name) then mq.delay('300ms') end
            return false, 'failed'
        end

        if not waitSlotHolds(best.name) then
            printf('\ar[PS]\ax Equip failed: the powersource slot does not hold "%s".', best.name)
            return false, 'failed'
        end
        printf("\ay[PS]\ax \agEquip complete! Now wearing %s.\ax", best.name)
        return true
    else
        -- Case B: slot has a spent (or forced wrong-role) PS - 3-step swap
        local oldName = live.name
        printf("\ay[PS]\ax Swapping \ar%s\ax (%s) -> \ag%s\ax (T%d)",
               oldName, live.spent and 'spent' or 'wrong role', best.name, best.tier)

        if not cursorIsEmpty('swap') then return false, 'busy' end
        -- Re-read right before the pickup: same item, same spent state as the decision was based on
        local slotNow = getEquippedPS()
        if not slotNow or slotNow.name ~= oldName or slotNow.spent ~= live.spent then
            printf('\ay[PS]\ax Swap skipped: powersource slot changed (no longer the "%s" we decided on).', oldName)
            return false, 'busy'
        end
        if uiBusy() then return false, 'busy' end
        local dlgBefore = confirmDialogOpen()   -- a dialog already open now is never ours

        -- /ctrl required to pick up an equipped item from the slot onto cursor
        mq.cmd("/ctrl /itemnotify powersource leftmouseup")
        mq.delay('300ms')
        if cursorName() ~= oldName then
            printf("\ar[PS]\ax Swap failed: could not pick up equipped PS.")
            return false, 'failed'
        end
        if busyRecover('swap', oldName) then return false, 'busy' end

        if not bagItemIs(best.bag, best.slot, best.name) then
            printf("\ar[PS]\ax Swap aborted: replacement vanished from bags - returning the old PS.")
            if safeAutoInventory('swap', oldName) then mq.delay('300ms') end
            return false, 'failed'
        end
        clickBagSlot(best.bag, best.slot)
        mq.delay('300ms')
        local onCursor = cursorName()
        if onCursor ~= best.name then
            printf('\ar[PS]\ax Swap failed: expected "%s" on cursor, found "%s".',
                   best.name, tostring(onCursor or 'nothing'))
            if safeAutoInventory('swap', oldName) then mq.delay('300ms') end
            return false, 'failed'
        end
        if busyRecover('swap', best.name) then return false, 'busy' end
        -- The old PS was taken out of the slot above: it must be empty now (or still the same spent item)
        if not slotStillExpected(oldName) then return equipRaceRecover('swap', best.name) end

        mq.cmd("/ctrl /itemnotify powersource leftmouseup")
        mq.delay('200ms')
        if clickAttuneConfirm(dlgBefore, best.name) then
            mq.delay('300ms')
        end

        if cursorName() then
            printf("\ar[PS]\ax Swap anomaly: cursor still has an item!")
            if safeAutoInventory('swap', best.name) then mq.delay('300ms') end
            return false, 'failed'
        end

        if not waitSlotHolds(best.name) then
            printf('\ar[PS]\ax Swap failed: the powersource slot does not hold "%s".', best.name)
            return false, 'failed'
        end
        printf("\ay[PS]\ax \agSwap complete! Now wearing %s.\ax", best.name)
        return true
    end
end

-- Guarded entry point: sets the swapping lock for the duration (including every mq.delay,
-- during which binds and actor callbacks may run) and always releases it.
local function trySwapPS(force, strict)
    if swapping then return false, 'busy' end
    swapping = true
    local ok, a, b = pcall(trySwapPSInner, force, strict)
    swapping = false
    if not ok then
        printf("\ar[PS]\ax Swap error: %s", tostring(a))
        return false, 'failed'
    end
    return a, b
end

-- ============================================================
-- Actors (cross-toon messaging; the module ships with MacroQuest)
-- ============================================================
-- Declared here (before the mailbox closure) so the callback captures
-- these locals as upvalues rather than writing to globals.
-- Handlers (binds, actor callbacks, ImGui) only set these flags; the main loop does the work,
-- because the work uses mq.delay and that must never run inside a callback.
local pendingForceSwap     = false  -- swap regardless of spent state (Swap button / remote request)
local pendingManualSwap    = false  -- /ps swap
local pendingManualConvert = false  -- /ps convert

-- Loose server-name comparison: the actor layer reports the server SHORT name while
-- EverQuest.Server is the long name, so compare lowercase alphanumerics and accept containment.
local function sameServer(a, b)
    local function norm(s) return (tostring(s):lower():gsub('[^%w]', '')) end
    a, b = norm(a), norm(b)
    if a == '' or b == '' then return false end
    return a == b or a:find(b, 1, true) ~= nil or b:find(a, 1, true) ~= nil
end

-- Validate the actor-layer sender address. What the actor layer supports (LuaActor.cpp
-- LuaMessage::Sender, mq-definitions actors.lua): message.sender is a table that may carry uuid,
-- pid, name, mailbox, account, server, character and absolute_mailbox. There is NO separate
-- 'script' field; the script is part of the mailbox string. A mailbox registered with
-- actors.register('ps_tracker') in script ps_tracker is "<script>:<name>" = "ps_tracker:ps_tracker"
-- (RegisterWithName); the bare script form "ps_tracker" is also accepted. These are matched as
-- COMPLETE strings, never as substrings. Rules:
--   * mailbox must be present and exactly one of the forms above (another script's mailbox, or
--     none at all, is rejected); a 'script' field, should one ever appear, must be 'ps_tracker';
--   * character must be present and a valid name (only then can a status establish a peer);
--   * server must be present and match this toon's server. A sender without a server is
--     unverified and rejected: the payload's own d.server claim is never used to widen trust.
-- LIMITATION: nothing here can prove these fields beyond what the actor layer reports, and the
-- exact sender mailbox string could not be confirmed in-game (UNVERIFIED), hence both forms.
-- Returns ok, characterName, serverName.
local OUR_SCRIPT    = 'ps_tracker'
-- 'lua:ps_tracker:ps_tracker' is the form actually seen in game ("lua:<script>:<mailbox>").
local OUR_MAILBOXES = { ['ps_tracker'] = true, ['ps_tracker:ps_tracker'] = true,
                        ['lua:ps_tracker:ps_tracker'] = true }

local function vetSender(message)
    local addr = message.sender
    if type(addr) ~= 'table' then return false end
    if addr.script ~= nil and addr.script ~= OUR_SCRIPT then return false end
    if type(addr.mailbox) ~= 'string' or not OUR_MAILBOXES[addr.mailbox] then
        -- Show the real value once a minute: if the actor layer formats mailbox names differently
        -- than expected, every peer message would otherwise be dropped with no explanation.
        logThrottled('mbx_mismatch', 60000,
            '\ay[PS]\ax Ignoring a message from mailbox "%s" (expected ps_tracker).', tostring(addr.mailbox))
        return false
    end
    if not validName(addr.character) then return false end
    if type(addr.server) ~= 'string' or addr.server == '' then
        logThrottled('srv_unverified', 60000,
            '\ay[PS]\ax Ignoring a message whose sender address has no server (cannot be verified).')
        return false
    end
    if not sameServer(addr.server, myServer) then
        logThrottled('srv_mismatch', 60000,
            '\ay[PS]\ax Ignoring a message from server "%s" (this toon is on "%s").',
            addr.server, myServer)
        return false
    end
    return true, addr.character, addr.server
end

mailbox = actors.register('ps_tracker', function(message)
    if not message or type(message.content) ~= 'table' then return end
    local d = message.content
    -- Character name of the sender as reported by the actor layer; used to reject
    -- spoofed names and requests from anyone who is not a known peer.
    local okSender, sender, senderServer = vetSender(message)
    if not okSender then return end

    -- Trade request: {type='do_trade', dest=name, item=name}
    -- We give a PS_DB item from our own bags to a known peer that actually needs it.
    -- The actor layer must name the sender, the sender must be a known peer, and a peer may
    -- only request a trade for itself (our own UI uses the local startTradeSM path instead).
    if d.type == 'do_trade' then
        local now = mq.gettime()
        if not sender or not isKnownPeer(sender) then
            printf('\ay[PS]\ax Trade request ignored: sender "%s" is not a known peer (not reporting yet, or the actor layer gave no sender name).',
                   tostring(sender))
            return
        end
        if d.dest ~= sender then
            printf("\ay[PS]\ax Trade request ignored: a peer may only request a trade for itself.")
            return
        end
        if opBusy() or now - lastDoTradeAt < DO_TRADE_GAP_MS then
            printf("\ay[PS]\ax Trade request ignored (busy or too soon after the last one).")
            return
        end
        if type(d.item) ~= 'string' or PS_DB[d.item] == nil then
            printf("\ay[PS]\ax Trade request ignored: the requested item is not a known Power Source.")
            return
        end
        if not isKnownPeer(d.dest) or not peerNeedsPS(peers[d.dest]) then
            printf("\ay[PS]\ax Trade request ignored: destination is unknown or does not need a PS.")
            return
        end
        if not roleMatches(psRole(d.item), peers[d.dest].role) then
            printf("\ay[PS]\ax Trade request ignored: that PS does not fit the destination's role.")
            return
        end
        if not findInBags(d.item, true) then
            printf('\ay[PS]\ax Trade request ignored: no tradeable "%s" in my bags (NO TRADE or spent).', d.item)
            return
        end
        if startTradeSM(d.dest, d.item) then
            lastDoTradeAt = now   -- only a started run blocks the next request
            printf("\ay[PS]\ax Trade request: give \at%s\ax to \at%s", d.item, d.dest)
        end
        return
    end
    -- Heads-up that a known peer is about to trade us an item:
    -- {type='expect_trade', from=name, item=name}. Only then is a trade auto-accepted.
    if d.type == 'expect_trade' then
        if sender and sender == d.from and isKnownPeer(d.from)
           and type(d.item) == 'string' and PS_DB[d.item] ~= nil then
            -- A window that is already open when the announcement arrives (e.g. a manual trade
            -- this script already ignored) must never be authorized by it: needFresh keeps the
            -- announcement dormant until that window has closed.
            local tw = mq.TLO.Window('TradeWnd')
            local id = type(d.id) == 'string' and #d.id <= 80 and d.id or nil
            -- one entry per giver (a new announcement from the SAME giver replaces its old one)
            pendingIncoming[d.from] = { from = d.from, item = d.item, at = mq.gettime(), id = id,
                                        needFresh = (tw and tw.Open()) and true or false }
            printf("\ay[PS]\ax Expecting \at%s\ax from \at%s", d.item, d.from)
            -- Tell the giver the announcement is registered, so it may open the window now.
            -- (Old versions never ack: their giver run is cancelled after TRADE_ACK_MS.)
            if id then
                mailbox:send(
                    { character=d.from, script='ps_tracker', mailbox='ps_tracker' },
                    { type='expect_ack', from=myName, id=id, server=myServer }
                )
            end
        else
            printf('\ay[PS]\ax Trade announcement ignored: sender "%s" does not match "%s", is not a known peer, or the item is unknown.',
                   tostring(sender), tostring(d.from))
        end
        return
    end
    -- The giver gave up on an announced trade: {type='cancel_trade', from=name}. Validated like
    -- expect_trade; it only clears an expectation that came from that same peer.
    -- When both sides carry a run id, the ids must match, so an old cancel cannot clear a newer
    -- announcement from the same peer.
    if d.type == 'cancel_trade' then
        local pit = sender and sender == d.from and type(d.from) == 'string' and pendingIncoming[d.from] or nil
        if pit and isKnownPeer(d.from)
           and not (pit.id and type(d.id) == 'string' and pit.id ~= d.id) then
            pendingIncoming[d.from] = nil   -- only that giver's entry
            printf("\ay[PS]\ax Trade from \at%s\ax was cancelled - no longer expecting it.", d.from)
        end
        return
    end
    -- The recipient registered our announcement: {type='expect_ack', from=name, id=run id}.
    -- Only the destination of the running trade, answering this run's id, counts.
    if d.type == 'expect_ack' then
        if sender and sender == d.from and tradeSM.active and tradeSM.announced
           and tradeSM.destName == sender and type(d.id) == 'string' and d.id == tradeSM.runId then
            tradeSM.acked = true
        end
        return
    end
    -- A peer is shutting down: {type='bye', name=name}. Only the peer itself can remove itself.
    if d.type == 'bye' then
        if sender and sender == d.name and validName(d.name) and d.name ~= myName then
            peers[d.name] = nil
            lastBye[d.name] = mq.gettime()   -- positive proof of a stop (see restartStep)
        end
        return
    end
    -- Swap request: {type='do_swap'} - from a known peer, and only acted on when our PS needs it.
    -- It only ever equips from our OWN bags; it never causes a trade.
    if d.type == 'do_swap' then
        if not sender or not isKnownPeer(sender) then
            printf('\ay[PS]\ax Swap request ignored: sender "%s" is not a known peer.', tostring(sender))
            return
        end
        local now = mq.gettime()
        if lastDoSwapAt ~= 0 and now - lastDoSwapAt < DO_SWAP_GAP_MS then return end
        lastDoSwapAt = now
        pendingForceSwap = true
        printf("\ay[PS]\ax Swap request received for \at%s\ax - searching for %s PS on next tick",
               myName, myRole)
        return
    end
    -- Normal status broadcast (no type field)
    if d.type ~= nil then return end
    if not validName(d.name) then return end
    if d.name == myName then return end
    -- A verified character is required: the status must come from the toon it names.
    if not sender or sender ~= d.name then return end
    local clean = cleanPeerStatus(d, senderServer)
    if clean then
        peers[d.name] = clean
        lastHeard[d.name] = clean.ts
    end
end)

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
    mailbox:send({ script='ps_tracker', mailbox='ps_tracker' }, buildStatus())
end

-- ============================================================
-- Find who can supply a PS to a needy toon.
-- Picks the highest-tier fresh item; within the same tier, an unsealed
-- copy beats a sealed one so we don't crack a sealed item unnecessarily.
-- Items that are NO TRADE are never offered; a stack is offered (one item is split off, see
-- stackPickStep) while STACK_TRADE_ENABLED.
-- Only sources THIS toon can actually initiate are considered: its own bags (when it is not
-- the recipient), or a peer's bags when this toon is the recipient (the Trade button only
-- appears in those two cases).
-- Returns: { peer=name, item=item, sealed=bool } or nil
-- ============================================================
local function findTradeSource(forRole, recipient)
    local best = nil
    local now  = mq.gettime()

    local function isBetter(candidate, cur)
        if not cur then return true end
        if candidate.tier ~= cur.item.tier then return candidate.tier > cur.item.tier end
        -- Same tier: prefer unsealed over sealed
        return not candidate.sealed and cur.item.sealed
    end
    local function giveable(item)
        return not item.spent and not item.noTrade
               and (STACK_TRADE_ENABLED or (item.stack or 1) <= 1)
               and roleMatches(item.role, forRole)
    end

    -- Check local bags (bagsCache always reflects the most recent scan)
    if recipient ~= myName then
        for _, item in ipairs(bagsCache) do
            if giveable(item) and isBetter(item, best) then
                best = { peer = myName, item = item, sealed = item.sealed }
            end
        end
    end

    -- Check peers (only when we are the one asking)
    if recipient == myName then
        for _, peer in pairs(peers) do
            if peer.name ~= myName and (now - (peer.ts or 0)) <= PEER_TIMEOUT_MS then
                for _, item in ipairs(peer.bags or {}) do
                    if giveable(item) and isBetter(item, best) then
                        best = { peer = peer.name, item = item, sealed = item.sealed }
                    end
                end
            end
        end
    end

    return best
end

-- A PS this toon needs is only held by a peer. Peers are never asked automatically:
-- log one throttled hint telling the user to click Trade in the overlay.
local function hintPeerSource(src, force)
    local now = mq.gettime()
    if not force and lastPeerHintAt ~= 0 and now - lastPeerHintAt < NOPS_WARN_GAP_MS then return end
    lastPeerHintAt = now
    printf("\ay[PS]\ax \at%s\ax holds a usable \at%s\ax - click Trade next to your name in the overlay to request it.",
           src.peer, src.item.name)
end

-- ============================================================
-- DanNet remote execute helpers - module level so renderUI and
-- the main loop can both call them. Only ever called from a button
-- the user clicked.
-- DanNet.Peers() names are fully qualified ("server_Toon", per the MQ2DanNet docs), so the
-- local toon is recognised by its short name (the part after the last underscore, compared
-- case-insensitively) and the ORIGINAL peer string is what is passed to /dex.
-- Stop uses "/ps stop" (the cooperative path: the peer leaves its main loop, runs its normal
-- shutdown, and says goodbye). "/lua stop" would kill the peer's coroutine and skip that cleanup.
-- ============================================================
local pendingRestart = nil   -- { targets = {{full=, short=}}, at = ms, goAt = ms }

local function dexTargets()
    local dn = mq.TLO.Plugin('MQ2DanNet')
    if not dn or dn.IsLoaded() ~= true then
        printf("\ay[PS]\ax DanNet plugin not loaded.")
        return nil
    end
    local out = {}
    local peersStr = mq.TLO.DanNet.Peers()
    if not peersStr or peersStr == '' then return out end
    local me = myName:lower()
    for peer in peersStr:gmatch('[^|]+') do
        peer = peer:match('^%s*(.-)%s*$')
        local short = peer and peer:match('([^_]+)$')
        -- fully-qualified DanNet name is "<server>_<character>": the part before the last
        -- underscore is the server (nil when the name carries none)
        local srv = peer and peer:match('^(.-)_[^_]+$')
        if srv == '' then srv = nil end
        -- letters/digits/underscore only: the string ends up inside a /dex command
        if peer and peer ~= '' and short and peer:match('^[%w_]+$') and short:lower() ~= me then
            table.insert(out, { full = peer, short = short, server = srv })
        end
    end
    return out
end

local function dexAll(cmd)
    local targets = dexTargets()
    if not targets then return nil end
    if #targets == 0 then
        printf("\ay[PS]\ax No other DanNet peers found (DanNet.Peers is empty) - nothing sent: %s", cmd)
        return targets
    end
    local names = {}
    for _, t in ipairs(targets) do
        mq.cmdf("/dex %s %s", t.full, cmd)
        names[#names + 1] = t.short
    end
    printf("\ag[PS]\ax Sent \"%s\" to %d peer(s): %s", cmd, #targets, table.concat(names, ', '))
    return targets
end

-- Peer-table key of the toon that is running ps_tracker right now and is the DanNet target
-- `t` (case-insensitive character name AND server, where the server comes from the actor layer's
-- sender address, not from the payload), or nil. "Running" = a status broadcast within
-- PEER_TIMEOUT_MS. A target whose DanNet name carries no server, or whose server differs from
-- the peer's verified server, never matches.
local function runningPeerName(t)
    if not t.server then return nil end
    local s = t.short:lower()
    local now = mq.gettime()
    for n, p in pairs(peers) do
        if n ~= myName and n:lower() == s and (now - (p.ts or 0)) <= PEER_TIMEOUT_MS
           and p.server and sameServer(t.server, p.server) then
            return n
        end
    end
    return nil
end

-- Restart All design (positive stop confirmation).
-- A peer that is merely ABSENT from the peer table is not proof that it stopped: it may not
-- have broadcast yet, may be on another server, or may run an OLD ps_tracker that obeys
-- "/ps stop" but never sends 'bye'. Remote script state is not readable here (mq.TLO.Lua is
-- local only; a DanNet observer would need the peer's cooperation and is asynchronous), so the
-- evidence is the peer's own actor traffic, which every version produces while it runs:
--   * only peers seen RUNNING when the button was clicked are restarted (others are skipped);
--   * a toon is matched to the DanNet target by character AND server (the actor layer's verified
--     sender server vs the "<server>_" part of the DanNet name); a target without a server part, or
--     whose short name is shared by two DanNet targets, is skipped with a message;
--   * stopped = a 'bye' arrived after the stop was sent and no status followed it (new versions,
--     immediate; the preferred rule). Only a LEGACY peer (never sent any 'bye', and heard within
--     RESTART_SILENT_MS at click time: an old version that obeys /ps stop but never says goodbye)
--     may instead be confirmed by silence, and then only when the silence window starts after the
--     stop was sent (a status after it restarts the window) and lasts RESTART_SILENT_MS = 3
--     broadcast intervals. A peer that has ever sent a 'bye' must send a new one, otherwise it is
--     skipped with a message asking the user to stop/start it by hand;
--   * right before "/lua run" the peer must still be silent (it may have been restarted by hand);
--   * which rule confirmed the stop is logged once per peer;
--   * a peer not confirmed stopped within RESTART_WAIT_MS is skipped with a message, so a second
--     instance is never started on top of a running one.
-- Residual risk: a peer whose whole client is frozen is silent but may still be running; its
-- queued "/lua run" would then be refused by MQ because that script name is already running
-- (UNVERIFIED).
local function peerStopped(t, now)
    local heard = lastHeard[t.name] or 0
    local bye   = lastBye[t.name] or 0
    if bye >= t.sentAt and bye >= heard then return 'bye' end
    if t.legacy and now - math.max(heard, t.sentAt) >= RESTART_SILENT_MS then return 'silence' end
    return nil
end

local function startRestart()
    if pendingRestart then return end
    local all = dexTargets()
    if not all or #all == 0 then return end
    local now = mq.gettime()
    -- Two DanNet targets with the same short name cannot be told apart reliably: skip both.
    local shortCount = {}
    for _, t in ipairs(all) do
        local k = t.short:lower()
        shortCount[k] = (shortCount[k] or 0) + 1
    end
    local targets = {}
    for _, t in ipairs(all) do
        if shortCount[t.short:lower()] > 1 then
            printf('\ay[PS]\ax %s is ambiguous (several DanNet peers share that name) - not restarted.', t.short)
        elseif not t.server then
            printf('\ay[PS]\ax %s has no server in its DanNet name - cannot be matched safely, not restarted.', t.short)
        else
            local name = runningPeerName(t)
            if name then
                t.name   = name
                t.sentAt = now
                -- legacy = an old version: never said goodbye, but was heard very recently
                t.legacy = lastBye[name] == nil and (now - (lastHeard[name] or 0)) <= RESTART_SILENT_MS
                table.insert(targets, t)
            else
                printf('\ay[PS]\ax %s is not reporting ps_tracker - not restarted (use Start All for a toon that is not running it).', t.short)
            end
        end
    end
    if #targets == 0 then return end
    for _, t in ipairs(targets) do
        mq.cmdf("/dex %s /ps stop", t.full)
    end
    pendingRestart = { targets = targets, at = now }
end

local function restartStep(now)
    local r = pendingRestart
    if not r then return end
    local open = 0
    for _, t in ipairs(r.targets) do
        if not t.done then
            if not t.stoppedAt then
                local rule = peerStopped(t, now)
                if rule then
                    t.stoppedAt = now
                    t.rule = rule
                    if rule == 'bye' then
                        printf('\ay[PS]\ax %s confirmed stopped (goodbye message received).', t.short)
                    else
                        printf('\ay[PS]\ax %s confirmed stopped by silence (old version without goodbye; no status for %ds after the stop).',
                               t.short, math.floor(RESTART_SILENT_MS / 1000))
                    end
                end
            end
            if t.stoppedAt then
                -- a moment for the old script to unload before the new one starts
                if now - t.stoppedAt >= 500 then
                    local heard = lastHeard[t.name] or 0
                    if heard > t.stoppedAt then
                        printf('\ar[PS]\ax %s is reporting again - not restarted.', t.short)
                    elseif t.rule == 'silence' and now - math.max(heard, t.sentAt) < RESTART_SILENT_MS then
                        printf('\ar[PS]\ax %s is no longer silent - not restarted.', t.short)
                    else
                        mq.cmdf("/dex %s /lua run ps_tracker", t.full)
                    end
                    t.done = true
                end
            elseif now - r.at >= RESTART_WAIT_MS then
                if t.legacy then
                    printf('\ar[PS]\ax %s did not stop (still reporting) - not restarted.', t.short)
                else
                    printf('\ar[PS]\ax %s did not confirm it stopped (no goodbye received) - not restarted; stop and start it once by hand.', t.short)
                end
                t.done = true
            end
        end
        if not t.done then open = open + 1 end
    end
    if open == 0 then pendingRestart = nil end
end

-- ============================================================
-- Auto-accept incoming trade + auto-equip after receipt.
-- Called each main-loop tick - fully non-blocking.
-- Equip is driven by equipSM (phase 0-4) so no inline mq.delay().
-- ============================================================

-- An equip that did not end with the intended item in the slot counts toward the automatic cap.
local function noteEquipFail(now, msg)
    printf("\ar[PS]\ax %s", msg)
    autoSwapFails  = autoSwapFails + 1
    lastAutoSwapAt = now
    if autoSwapFails == SWAP_MAX_FAILS then
        printf("\ar[PS]\ax Auto-equip failed %d times - pausing automatic attempts. Use /ps swap to retry.",
               autoSwapFails)
    end
end

-- Would equipping `name` still help, judged against what is in the slot RIGHT NOW?
-- Yes when the slot is empty, spent or holds a wrong-role PS, or when `name` is a higher tier.
local function equipWouldHelp(name)
    local live = getEquippedPS()
    if not live or live.spent or not roleMatches(live.role, myRole) then return true end
    return psTier(name) > live.tier
end

local function itemKey(it) return ('%d:%d:%s'):format(it.bag, it.slot, it.name) end

-- Count of unsealed items named `name` in a getPSInBags() list.
local function countNamed(list, name)
    local n = 0
    for _, it in ipairs(list) do
        if it.name == name and not it.sealed then n = n + 1 end
    end
    return n
end

-- After a Convert click: the unsealed result must carry the name derived from the sealed item
-- (equipSM.expectName), the bags must hold MORE copies of it than the pre-click snapshot
-- (so an unrelated unsealed source that merely moved bag slots is never credited), and it must be
-- at the original slot or at a position that was not occupied by that name before the click.
-- Anything else is a failed convert. (A result on the cursor is judged by the callers.)
local function findConvertResult(list)
    local b = equipSM.best
    local want = equipSM.expectName
    if not b or not want or equipSM.beforeCount == nil then return nil end
    if countNamed(list, want) <= equipSM.beforeCount then return nil end
    for _, it in ipairs(list) do
        if it.name == want and not it.sealed and it.bag == b.bag and it.slot == b.slot then return it end
    end
    if equipSM.before then
        for _, it in ipairs(list) do
            if it.name == want and not it.sealed and not equipSM.before[itemKey(it)] then return it end
        end
    end
    return nil
end

-- A convert result found on the cursor: it must carry the expected unsealed name AND the
-- accounting must show a conversion happened: the unsealed count (bags + this cursor item) rose
-- versus the pre-click snapshot, or the sealed source is gone/changed. Otherwise the cursor item is
-- merely something with the same name that was picked up from elsewhere (never credited).
local function cursorConvertOk(onCursor)
    local want, b = equipSM.expectName, equipSM.best
    if not want or not b or onCursor ~= want or equipSM.beforeCount == nil then return false end
    if countNamed(getPSInBags(), want) + 1 > equipSM.beforeCount then return true end
    return not bagItemIs(b.bag, b.slot, b.name)
end

-- Name of the trade partner shown in an open TradeWnd, or ''. The game's own UI file
-- (uifiles/default/EQUI_TradeWnd.xml) names the partner label TRDW_OtherName (an STMLbox);
-- TRDW_HisName does not exist there and reading Text on it crashed the script. Every lookup is
-- guarded with pcall so a custom UI without these controls yields '' (never an error), and
-- ''-partner windows are never auto-accepted. Any markup in the STML text is stripped.
local function tradePartnerName(tw)
    if not tw then return '' end
    for _, ctlName in ipairs({ 'TRDW_OtherName', 'TRDW_HisName' }) do
        local txt = ctlText(tw, ctlName)
        if txt then
            txt = txt:gsub('<[^>]*>', ''):match('^%s*(.-)%s*$')
            if txt ~= '' then return txt end
        end
    end
    return ''
end

-- Cancel an open TradeWnd without blocking: the first call clicks TRDW_Cancel_Button (which sends
-- the cancel to the server); a repeat call (>= 500 ms later, callers poll) falls back to
-- Window.DoClose if the window is still open. Returns true when a window was open.
local cancelClickAt = 0
local function cancelTradeWnd()
    local tw = mq.TLO.Window('TradeWnd')
    if not (tw and tw.Open()) then
        cancelClickAt = 0
        return false
    end
    local now = mq.gettime()
    if cancelClickAt == 0 or now - cancelClickAt > 8000 then
        mq.cmd('/notify TradeWnd TRDW_Cancel_Button leftmouseup')
        cancelClickAt = now
    elseif now - cancelClickAt >= 500 then
        tw.DoClose()
    end
    return true
end

local function autoAcceptStep()
    local now       = mq.gettime()
    local tradeWnd  = mq.TLO.Window('TradeWnd')
    local tradeOpen = tradeWnd and tradeWnd.Open()

    -- ---- Auto-accept ----------------------------------------
    -- Window-open edge (whoever opened the window): first tick on which it is seen open
    if tradeOpen and tradeWndOpenAt == 0 then tradeWndOpenAt = now end
    -- Expire a stale expectation
    for giver, e in pairs(pendingIncoming) do
        if now - e.at > PENDING_TRADE_TTL_MS then pendingIncoming[giver] = nil end
    end

    if not tradeOpen then
        -- The window we accepted is gone: schedule the post-trade equip scan (only now, not at
        -- click time). If the giver cancelled instead, the scan simply finds no upgrade.
        if tradeAccepted then
            postTradeEquipAt = now + 2000
            postTradeRetries = 0
        end
        -- Window closed: clear all per-trade flags (including the rejection latch). An
        -- announcement that arrived while a window was open (needFresh) is KEPT, with its TTL, and
        -- becomes usable for the next window. An announcement that was usable for the window that
        -- just closed (that partner's, not needFresh) was not consumed by an accept: it is dropped.
        for giver, e in pairs(pendingIncoming) do
            if e.needFresh then
                e.needFresh = false
            elseif tradeWasOpen and giver == tradeWndPartner then
                pendingIncoming[giver] = nil
            end
        end
        tradeWndPartner     = ''
        tradeAcceptedPartner = ''
        tradeWndOpenAt      = 0
        tradeWasOpen        = false
        tradeRejectLogged   = false
        tradeRecvAt         = 0
        tradeAccepted       = false
        tradeAcceptedAt     = 0
    elseif not tradeSM.active and not tradeAccepted then
        -- TradeWnd is open but WE did not initiate it. Only accept when the other
        -- party is a known peer AND that peer announced this trade (expect_trade).
        tradeWasOpen = true
        local hisName = tradePartnerName(tradeWnd)
        -- An announcement authorizes only a window that opened AFTER it was received
        -- (needFresh covers a window that was already open at receipt; the timestamps cover a
        -- window whose open edge was seen before the announcement). A window rejected once stays
        -- rejected (latched in tradeRejectLogged) until it closes, so a later announcement can
        -- never retroactively authorize it.
        -- The announcement is the one stored for THIS partner; other givers' entries are untouched.
        if hisName ~= '' then tradeWndPartner = hisName end
        local pit = hisName ~= '' and pendingIncoming[hisName] or nil
        local trusted = hisName ~= '' and not tradeRejectLogged and isKnownPeer(hisName)
                        and pit ~= nil and pit.from == hisName
                        and not pit.needFresh and tradeWndOpenAt >= pit.at
        if not trusted then
            tradeRecvAt = 0
            -- An empty partner name may just mean the window is still filling in: retry, do not latch.
            if hisName ~= '' and not tradeRejectLogged then
                tradeRejectLogged = true
                printf('\ay[PS]\ax Ignoring trade window from "%s" (not an expected peer trade)', hisName)
            end
        elseif tradeRecvAt == 0 then
            tradeRecvAt = now          -- start the 1-second delay timer
        elseif now - tradeRecvAt >= 1000 then
            local tradeBtn = tradeWnd.Child('TRDW_Trade_Button')
            if tradeBtn and tradeBtn() then
                printf("\ay[PS]\ax Auto-accepting incoming trade")
                tradeBtn.LeftMouseUp()
                -- Only a click that really happened counts as accepted; the announcement is
                -- single-use, and the post-trade equip is scheduled when the window closes.
                tradeAccepted        = true
                tradeAcceptedAt      = now
                tradeAcceptedPartner = hisName
                pendingIncoming[hisName] = nil   -- single use: only this giver's entry is consumed
                tradeRecvAt          = 0
            else
                logThrottled('trade_btn', 10000, "\ar[PS]\ax Incoming trade: TRDW_Trade_Button not found - cannot accept.")
            end
        end
    elseif not tradeSM.active and tradeAccepted and tradeAcceptedAt > 0
           and now - tradeAcceptedAt > TRADE_STUCK_MS then
        -- We accepted but the window never closed (the giver never confirmed): cancel it.
        logThrottled('trade_stuck', 30000, "\ay[PS]\ax Accepted trade did not complete within %ds - cancelling the trade window.",
                     math.floor(TRADE_STUCK_MS / 1000))
        cancelTradeWnd()
        -- re-check shortly: a second attempt falls back to DoClose if the window is still open
        tradeAcceptedAt = now - TRADE_STUCK_MS + 1500
    end

    -- ---- Auto-equip phases 0-4 (pcall-wrapped so a crash inside logs an
    --      error message but does NOT kill the whole script)
    local equipOk, equipErr = pcall(function()

    -- Bounded reschedule of the post-trade equip (cursor busy, slot changed, ...)
    local function reschedulePostTrade(reason)
        postTradeRetries = postTradeRetries + 1
        if postTradeRetries <= POST_TRADE_RETRIES then
            postTradeEquipAt = now + 2000
            printf("\ay[PS]\ax Auto-equip: %s - will check again shortly (%d/%d).",
                   reason, postTradeRetries, POST_TRADE_RETRIES)
        else
            postTradeEquipAt = 0
            printf("\ay[PS]\ax Auto-equip: %s - giving up on the post-trade equip.", reason)
        end
    end

    -- ---- Auto-equip phase 0: scan + send pickup command -----
    -- (waits, keeping the schedule, while a trade/merchant/bank/loot window is still open)
    if equipSM.phase == 0 and postTradeEquipAt > 0 and now >= postTradeEquipAt
       and not tradeSM.active and not swapping and not uiBusy() then

        -- Refresh caches so we see the newly received item
        equippedCache = getEquippedPS()
        bagsCache     = getPSInBags()

        -- Worn tier counts only when it is fresh AND fits the role: a wrong-role stopgap
        -- or a spent PS counts as tier 0, so a correct-role PS of any tier replaces it.
        local curTier = (equippedCache and equippedSuits(equippedCache)) and equippedCache.tier or 0

        -- Find the best non-spent PS for our role that beats current.
        -- Sealed items are included; a convert phase handles them before equip.
        local best = nil
        for _, item in ipairs(bagsCache) do
            if not item.spent and roleMatches(item.role, myRole) and item.tier > curTier then
                best = item
                break   -- bagsCache is already sorted best-first (tier, then unsealed before sealed)
            end
        end

        if best then
            -- Picking up another item with a full cursor drops or merges items
            -- unpredictably: do not touch it, try again a few times.
            if cursorName() then
                reschedulePostTrade(('cursor holds "%s"'):format(cursorName()))
                return
            end
            if best.sealed then
                -- Sealed item: open ItemDisplayWindow via Inspect(), then phase 1
                -- clicks IDW_ConvertButton to convert it.  The old rightmouseup approach
                -- failed with "Could not find slot to send notification to" because
                -- /itemnotify in packN slotM requires the bag window to be open;
                -- Inspect() bypasses that restriction entirely.
                printf("\ay[PS]\ax Auto-equip: converting sealed \ag%s\ax (T%d) in pack%d slot%d",
                       best.name, best.tier, best.bag, best.slot)
                if startConvert(best) then
                    postTradeEquipAt = 0
                    postTradeRetries = 0
                else
                    reschedulePostTrade("convert could not start")
                end
            else
                -- Non-sealed: pick up directly (after confirming the slot still holds it)
                if not bagItemIs(best.bag, best.slot, best.name) then
                    reschedulePostTrade(('"%s" is no longer at pack%d slot %d'):format(best.name, best.bag, best.slot))
                    return
                end
                postTradeEquipAt = 0
                postTradeRetries = 0
                printf("\ay[PS]\ax Auto-equip: picking up \ag%s\ax (T%d) from pack%d slot%d",
                       best.name, best.tier, best.bag, best.slot)
                -- Pick the new PS up from the bag slot onto the cursor
                clickBagSlot(best.bag, best.slot)
                equipSM.best   = best
                equipSM.expect = best.name
                equipSM.phase  = 2          -- cursor-check + equip phase (skips convert)
                equipSM.nextAt = now + 400  -- wait for pickup animation
            end
        else
            postTradeEquipAt = 0
            postTradeRetries = 0
            printf("\ay[PS]\ax Auto-equip: no upgrade found in bags (curTier=%d, role=%s)",
                   curTier, myRole)
        end
    end

    -- ---- A trade/merchant/bank/loot window opened while the convert was waiting ----
    if equipSM.phase == 1 and now >= equipSM.nextAt and uiBusy() then
        printf("\ay[PS]\ax Auto-equip: a trade/merchant/bank/loot window opened - stopping the convert.")
        resetEquipSM()   -- also closes ItemDisplayWindow
    end

    -- ---- Auto-equip phase 1: convert sealed item via ItemDisplayWindow ----
    -- Step A: wait for the window to open, then click IDW_ConvertButton.
    -- Step B: once convertBtnClicked is true, answer any confirmation dialog, then find the
    -- unsealed result (original slot or a new item) and pick it up.
    if equipSM.phase == 1 and now >= equipSM.nextAt then
        if not equipSM.convertBtnClicked then
            -- Step A: click the convert button
            local idw = mq.TLO.Window('ItemDisplayWindow')
            if idw and idw.Open() then
                local btn = idw.Child('IDW_ConvertButton')
                if btn and btn() and btn.Enabled() then
                    -- Convert is irreversible, so be strict: the bag slot must still hold the
                    -- intended sealed item AND the window must still show exactly that item
                    -- (IDW_ItemInfo1 holds the item name; the window may have been replaced by
                    -- another Inspect). Anything else aborts without clicking.
                    local shownName = (ctlText(idw, 'IDW_ItemInfo1') or ''):gsub('<[^>]*>', ''):match('^%s*(.-)%s*$')
                    if not bagItemIs(equipSM.best.bag, equipSM.best.slot, equipSM.best.name) then
                        printf("\ar[PS]\ax Auto-equip: the sealed item moved before the click - aborting convert.")
                        noteConvertFail(now)
                        resetEquipSM()
                    elseif shownName == '' and equipSM.convertAttempts <= 10 then
                        -- window open but its text not filled in yet: poll again (bounded)
                        equipSM.convertAttempts = equipSM.convertAttempts + 1
                        equipSM.nextAt = now + 300
                    elseif shownName ~= equipSM.best.name then
                        printf('\ar[PS]\ax Auto-equip: ItemDisplayWindow shows "%s", not "%s" - not clicking Convert.',
                               shownName, equipSM.best.name)
                        noteConvertFail(now)
                        resetEquipSM()
                    else
                        -- Remember the unsealed PS items that exist now, to recognise the result
                        equipSM.before = {}
                        local snap = getPSInBags()
                        for _, it in ipairs(snap) do
                            if not it.sealed then equipSM.before[itemKey(it)] = true end
                        end
                        -- (an unsealed copy already on the cursor counts too: bags + cursor)
                        equipSM.beforeCount = countNamed(snap, equipSM.expectName)
                                              + (cursorName() == equipSM.expectName and 1 or 0)
                        equipSM.dlgBefore = confirmDialogOpen()   -- a dialog open now is never ours
                        printf("\ay[PS]\ax Auto-equip: clicking IDW_ConvertButton to convert sealed item")
                        btn.LeftMouseUp()
                        idw.DoClose()
                        equipSM.convertBtnClicked = true
                        equipSM.convertAttempts   = 0
                        equipSM.attuneClicks      = 0
                        equipSM.nextAt            = now + 1500  -- wait for conversion to complete
                    end
                else
                    printf("\ar[PS]\ax Auto-equip: IDW_ConvertButton not found/enabled in ItemDisplayWindow - aborting.")
                    noteConvertFail(now)
                    resetEquipSM()   -- also closes ItemDisplayWindow
                end
            else
                -- Window not open yet - keep polling (up to ~3 s)
                equipSM.convertAttempts = equipSM.convertAttempts + 1
                if equipSM.convertAttempts > 10 then
                    printf("\ar[PS]\ax Auto-equip: ItemDisplayWindow did not open (timeout) - aborting.")
                    noteConvertFail(now)
                    resetEquipSM()
                else
                    equipSM.nextAt = now + 300  -- poll every 300 ms
                end
            end
        elseif equipSM.attuneClicks < 5
               and clickAttuneConfirm(equipSM.dlgBefore, equipSM.best.name, equipSM.expectName) then
            -- Convert raised a confirmation dialog about attuning/binding: answered it (bounded);
            -- judge the result on a later tick.
            equipSM.attuneClicks = equipSM.attuneClicks + 1
            equipSM.nextAt       = now + 600
        else
            -- Step B: conversion complete - EQ may have placed the item on the cursor
            -- directly (common behavior), or it may be back in the original bag slot.
            local onCursor = cursorName()
            if equipSM.convertOnly then
                -- Manual /ps convert: do not equip. The result is verified (unsealed PS_DB item on
                -- the cursor, or at the original slot / a new item in the bags) before success is
                -- reported. A converted PS on the cursor is put away (safeAutoInventory only
                -- moves PS_DB items).
                local verified = nil
                if onCursor then
                    -- Only the exact expected unsealed item counts as the result, and only with
                    -- snapshot evidence (see cursorConvertOk); anything else on the cursor is left
                    -- alone (safeAutoInventory also refuses non-PS items).
                    if cursorConvertOk(onCursor) then
                        verified = onCursor
                        safeAutoInventory('convert', equipSM.expectName)
                    else
                        printf('\ar[PS]\ax Convert: cursor holds "%s" without proof of a conversion (expected "%s") - leaving it alone.',
                               onCursor, tostring(equipSM.expectName))
                    end
                    -- no bag rescan here: the item has not landed in a bag yet
                else
                    bagsCache = getPSInBags()
                    local res = findConvertResult(bagsCache)
                    verified = res and res.name or nil
                end
                if verified then
                    printf('\ay[PS]\ax Convert finished - \ag%s\ax stays in your bags (not equipped by this command).', verified)
                    -- the next automatic scan could equip it right away: pause that briefly
                    autoEquipPausedUntil = now + 2 * SCAN_MS
                else
                    printf("\ar[PS]\ax Convert did not produce an unsealed item - nothing was converted.")
                    noteConvertFail(now)
                end
                resetEquipSM()
            elseif onCursor and not cursorConvertOk(onCursor) then
                -- Not the item the sealed one turns into, or no proof that it came from the
                -- conversion (unsealed count did not rise and the sealed source is unchanged):
                -- never equip or move it
                printf('\ar[PS]\ax Auto-equip: cursor holds "%s" without proof of a conversion (expected "%s") - leaving it alone; aborting.',
                       onCursor, tostring(equipSM.expectName))
                noteConvertFail(now)
                resetEquipSM()
            elseif onCursor then
                -- Already on cursor - phase 2 verifies it is a usable PS before equipping
                printf('\ay[PS]\ax Auto-equip: converted item on cursor ("%s") - equipping', onCursor)
                equipSM.convertBtnClicked = false
                equipSM.convertAttempts   = 0
                equipSM.expect = equipSM.expectName
                equipSM.phase  = 2
                equipSM.nextAt = now + 100
            else
                -- Not on cursor - rescan bags
                bagsCache = getPSInBags()
                -- The result must be at the original slot or be a new item: a PS that was already
                -- in the bags is never credited as the conversion result.
                local converted = findConvertResult(bagsCache)
                if not converted then
                    local still = bagItem(equipSM.best.bag, equipSM.best.slot)
                    if still and still.Name() == equipSM.best.name then
                        printf("\ar[PS]\ax Auto-equip: convert did not take effect - aborting.")
                    else
                        printf("\ar[PS]\ax Auto-equip: no converted item found - aborting.")
                    end
                    noteConvertFail(now)
                    resetEquipSM()
                else
                    -- Only pick the converted item up when it fits our role and is a real
                    -- upgrade over what is worn (spent, empty or wrong-role slot counts as 0).
                    local eqNow   = getEquippedPS()
                    local curTier = (eqNow and equippedSuits(eqNow)) and eqNow.tier or 0
                    if converted.spent or not roleMatches(converted.role, myRole) or converted.tier <= curTier then
                        printf("\ay[PS]\ax Auto-equip: converted item is not a role match or not an upgrade (worn tier %d) - leaving it in the bag.",
                               curTier)
                        resetEquipSM()
                    elseif uiBusy() then
                        printf("\ay[PS]\ax Auto-equip: a trade/merchant/bank/loot window opened - leaving the converted item in the bag.")
                        resetEquipSM()
                    elseif cursorName() or not bagItemIs(converted.bag, converted.slot, converted.name) then
                        printf("\ay[PS]\ax Auto-equip: cursor or bag changed - leaving the converted item in the bag.")
                        resetEquipSM()
                    else
                        printf("\ay[PS]\ax Auto-equip: conversion done, picking up \ag%s\ax (T%d)",
                               converted.name, converted.tier)
                        clickBagSlot(converted.bag, converted.slot)
                        equipSM.best              = converted
                        equipSM.expect            = converted.name
                        equipSM.convertBtnClicked = false
                        equipSM.convertAttempts   = 0
                        equipSM.phase  = 2
                        equipSM.nextAt = now + 400
                    end
                end
            end
        end
    end

    -- ---- Auto-equip phase 2: equip cursor item -> powersource slot ----
    if equipSM.phase == 2 and now >= equipSM.nextAt then
        local held = cursorName()
        printf("\ay[PS]\ax Auto-equip phase 2: cursor after pickup = %s", tostring(held or 'empty'))
        if uiBusy() then
            -- A window that owns the cursor opened during the wait: put our item away, do not equip.
            printf("\ay[PS]\ax Auto-equip: a trade/merchant/bank/loot window opened - not equipping.")
            if held then safeAutoInventory('auto-equip', equipSM.expect) end
            resetEquipSM()
        elseif not held then
            printf("\ar[PS]\ax Auto-equip: pickup step failed - cursor is empty; aborting.")
            if equipSM.fromConvert then noteConvertFail(now) end
            resetEquipSM()
        else
            -- Only equip the exact item we picked up (or, after a convert, an unsealed PS_DB
            -- item that fits our role).
            local expectedOk
            if equipSM.expect then
                expectedOk = (held == equipSM.expect)
            else
                expectedOk = PS_DB[held] ~= nil and not psSealed(held)
                             and roleMatches(psRole(held), myRole)
            end
            if not expectedOk then
                printf('\ar[PS]\ax Auto-equip: cursor holds unexpected "%s" - not equipping; aborting.', held)
                safeAutoInventory('auto-equip', nil)   -- only moves it if it is a PS_DB item
                if equipSM.fromConvert then noteConvertFail(now) end
                resetEquipSM()
            elseif psSealed(held) or isItemSpent(mq.TLO.Cursor) then
                -- A converted result that is still sealed or already spent is not a usable PS
                printf('\ar[PS]\ax Auto-equip: "%s" on the cursor is sealed or spent - not equipping.', held)
                safeAutoInventory('auto-equip', held)
                if equipSM.fromConvert then noteConvertFail(now) end
                resetEquipSM()
            elseif not equipWouldHelp(held) then
                -- The live slot already holds an equal or better PS that fits the role
                printf('\ay[PS]\ax Auto-equip: "%s" is no longer an upgrade over the worn PS - putting it back.', held)
                safeAutoInventory('auto-equip', held)
                resetEquipSM()
            else
                -- Remember what is in the slot now: if anything, it lands on the cursor
                local psBefore = mq.TLO.Me.Inventory('powersource')
                equipSM.priorPS = (psBefore and psBefore() and psBefore.Name()) or nil
                equipSM.intended = held
                equipSM.attuneClicks = 0
                equipSM.dlgBefore = confirmDialogOpen()   -- a dialog open now is never ours
                -- /ctrl bypasses the attune confirmation dialog for attunable PS items
                printf("\ay[PS]\ax Auto-equip: firing /ctrl /itemnotify powersource leftmouseup")
                mq.cmd("/ctrl /itemnotify powersource leftmouseup")
                equipSM.phase  = 3
                equipSM.nextAt = now + 400   -- wait for equip animation
            end
        end
    end

    -- ---- Auto-equip phase 3: handle attune dialog, then check cursor ----
    if equipSM.phase == 3 and now >= equipSM.nextAt then
        -- Some items (e.g. Luclinite) show a confirmation dialog even with /ctrl.
        -- Click Yes (a bounded number of times) and loop back for another tick.
        if equipSM.attuneClicks < 5 and clickAttuneConfirm(equipSM.dlgBefore, equipSM.intended) then
            equipSM.attuneClicks = equipSM.attuneClicks + 1
            equipSM.nextAt = now + 400   -- wait for dialog to close & equip to register
        else
            local psNow  = mq.TLO.Me.Inventory('powersource')
            -- Nil-guard: psNow is an item object; psNow() checks slot validity;
            -- psNow.Name() is only called when the slot is populated.
            local psName = (psNow and psNow() and psNow.Name()) or 'empty'
            printf("\ay[PS]\ax Auto-equip phase 3: powersource slot = %s", psName)
            if cursorName() then
                -- The old PS (or something unexpected) is on the cursor: safeAutoInventory
                -- logs it and only moves it when it is a PS_DB item or the PS we swapped out.
                if safeAutoInventory('auto-equip', equipSM.priorPS) then
                    equipSM.phase  = 4
                    equipSM.nextAt = now + 400   -- wait for bag placement
                else
                    printf("\ar[PS]\ax Auto-equip: unexpected cursor item - aborting.")
                    resetEquipSM()
                end
            else
                -- Nothing on cursor - equip went clean, no old PS to stow. Verify the result.
                local intended = equipSM.intended
                equippedCache = getEquippedPS()
                bagsCache     = getPSInBags()
                if peers[myName] then
                    peers[myName].equipped = equippedCache
                    peers[myName].bags     = sanitizeBags(bagsCache)
                end
                if intended and psName ~= intended then
                    noteEquipFail(now, ('Auto-equip failed: the powersource slot holds "%s", expected "%s".')
                                       :format(psName, intended))
                else
                    printf("\ay[PS]\ax Auto-equip: equip complete (no old PS on cursor)")
                end
                resetEquipSM()
            end
        end
    end

    -- ---- Auto-equip phase 4: post-autoinventory cache refresh and verification ----
    if equipSM.phase == 4 and now >= equipSM.nextAt then
        local psNow  = mq.TLO.Me.Inventory('powersource')
        -- Nil-guard: same pattern as phase 3 - only dereference Name() when valid
        local psName = (psNow and psNow() and psNow.Name()) or 'empty'
        local intended = equipSM.intended
        if intended and psName ~= intended then
            noteEquipFail(now, ('Auto-equip failed: the powersource slot holds "%s", expected "%s".')
                               :format(psName, intended))
        else
            printf("\ay[PS]\ax Auto-equip complete: powersource = \ag%s\ax", psName)
        end
        local leftover = cursorName()
        if leftover then
            printf('\ar[PS]\ax Auto-equip: "%s" is still on the cursor (bags full?) - leaving it there.', leftover)
        end
        equippedCache = getEquippedPS()
        bagsCache     = getPSInBags()
        if peers[myName] then
            peers[myName].equipped = equippedCache
            peers[myName].bags     = sanitizeBags(bagsCache)
        end
        resetEquipSM()
    end

    end)  -- end pcall wrapper for equipSM
    if not equipOk then
        printf("\ar[PS]\ax equipSM crash (phase=%d): %s - resetting SM",
               equipSM.phase, tostring(equipErr))
        -- A crash during a convert counts as a failed convert (3-fail cap and retry gap apply)
        if equipSM.phase == 1 or equipSM.fromConvert then noteConvertFail(now) end
        if equipSM.phase >= 2 then
            -- Best-effort: put away our own item if it is still on the cursor
            pcall(safeAutoInventory, 'crash recovery', equipSM.priorPS)
        end
        resetEquipSM()
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
    -- Same branch as isItemSpent: MaxPower > 0 means the Power mechanic, otherwise Charges
    if eq.maxPower ~= nil and eq.maxPower > 0 then
        return math.floor((eq.power or 0) / eq.maxPower * 100) .. '%'
    end
    if eq.charges == nil or eq.charges == -1 then return 'perm' end
    return tostring(eq.charges)
end

-- ============================================================
-- ImGui render callback (never blocks: no mq.delay in here)
-- ============================================================
local function renderUI()
    if not showUI then return end

    imgui.SetNextWindowSize(880, 290, ImGuiCond.FirstUseEver)
    local winFlags = ImGuiWindowFlags.HorizontalScrollbar
    -- imgui.Begin returns (pOpen, shouldDraw): update showUI from the X-button state,
    -- check shouldDraw so we skip rendering when collapsed. End() is always called after Begin().
    local draw
    showUI, draw = imgui.Begin("Power Source Tracker##ps_tracker", showUI, winFlags)
    if not draw then
        imgui.End()
        return
    end

    if imgui.SmallButton("Hide##ps") then showUI = false end
    imgui.SameLine()

    -- Start/Stop/Restart All send a command to every DanNet peer; they only
    -- ever run when the user clicks the button.
    if imgui.SmallButton("Start All##ps") then
        dexAll("/lua run ps_tracker")
    end
    imgui.SameLine()
    -- Cooperative stop ("/ps stop"): the peer leaves its main loop and runs its normal shutdown
    -- (cancels trades, puts items away, says goodbye). "/lua stop" would kill its coroutine and
    -- skip all of that, so it is not used. The clicking toon itself is never targeted.
    if imgui.SmallButton("Stop All##ps") then
        dexAll("/ps stop")
    end
    imgui.SameLine()
    -- Restart All: ask the running peers to stop now; the main loop starts each one again once it
    -- is confirmed stopped (see restartStep).  mq.delay() must NOT be called here
    -- (ImGui callbacks are non-yieldable).
    if imgui.SmallButton("Restart All##ps") and pendingRestart == nil then
        startRestart()
    end
    imgui.SameLine()
    textCol(C_WARN, "Power Source Tracker")

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
        imgui.TableSetupColumn("Name",           ImGuiTableColumnFlags.WidthFixed,   95)
        imgui.TableSetupColumn("Class",          ImGuiTableColumnFlags.WidthFixed,   40)
        imgui.TableSetupColumn("Power Source",   ImGuiTableColumnFlags.WidthStretch)
        imgui.TableSetupColumn("Tier",           ImGuiTableColumnFlags.WidthFixed,   32)
        imgui.TableSetupColumn("Power",          ImGuiTableColumnFlags.WidthFixed,   52)
        imgui.TableSetupColumn("Status / Notes", ImGuiTableColumnFlags.WidthStretch)
        imgui.TableHeadersRow()

        local now = mq.gettime()
        for _, entry in ipairs(list) do
            if not entry or not entry.name then goto continue end
            local stale = (now - (entry.ts or now)) > PEER_TIMEOUT_MS
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
            textCol(col, eq and eq.name or "None")

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
                local wSrc = findTradeSource(entry.role or 'melee', entry.name)
                if imgui.IsItemHovered() then
                    imgui.BeginTooltip()

                    -- Swap path
                    imgui.TextColored(1, 1, 0, 1, "[Swap]")
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
                        imgui.Text(("Equip %s (T%d) from own bags"):format(localPS.name, localPS.tier))
                    else
                        imgui.TextDisabled("No suitable PS in its own bags (use Trade)")
                    end

                    imgui.Separator()

                    -- Trade path
                    imgui.TextColored(1, 1, 0, 1, "[Trade]")
                    imgui.SameLine()
                    if wSrc then
                        local shortName = wSrc.item.name:gsub('^Sealed ', ''):gsub(' Source$', '')
                        local sealNote = wSrc.item.sealed and " -> %s converts -> equips" or " -> %s equips"
                        sealNote = sealNote:format(entry.name)
                        if wSrc.peer == myName then
                            imgui.Text(("Trade %s T%d from own bags to %s%s"):format(shortName, wSrc.item.tier, entry.name, sealNote))
                        else
                            imgui.Text(("%s navs to %s -> trades %s T%d%s"):format(wSrc.peer, entry.name, shortName, wSrc.item.tier, sealNote))
                        end
                    else
                        imgui.TextDisabled("No PS available to trade")
                    end

                    imgui.EndTooltip()
                end
                imgui.SameLine()
                if imgui.SmallButton(("Swap##sw_%s"):format(entry.name)) then
                    if entry.name == myName then
                        -- local toon: set deferred flag (cannot call mq.delay from ImGui)
                        pendingForceSwap = true
                    else
                        -- remote toon: send actor message
                        mailbox:send(
                            {character=entry.name, script='ps_tracker', mailbox='ps_tracker'},
                            {type='do_swap', server=myServer}
                        )
                    end
                end
            end
            local needsSource = not eq or eq.spent or (eq and not roleMatches(eq.role, entry.role or 'melee'))
            if needsSource then
                local src = findTradeSource(entry.role or 'melee', entry.name)
                -- Never offer a trade to ourselves (a toon's own bags are handled by Swap/auto-equip)
                -- A peer may only request a trade for itself, so a trade out of a peer's bags
                -- can only be requested from the recipient's own overlay.
                if src and src.peer ~= entry.name and (src.peer == myName or entry.name == myName) then
                    imgui.SameLine()
                    if imgui.SmallButton(("Trade##tr_%s"):format(entry.name)) then
                        if src.peer == myName then
                            -- Source is in our own bags - start tradeSM directly
                            if startTradeSM(entry.name, src.item.name) then
                                printf("\ay[PS]\ax Trade (local): giving \at%s\ax to \at%s",
                                    src.item.name, entry.name)
                            end
                        else
                            -- Source is on a peer - send actor message. The giver validates the
                            -- request and announces the trade to the recipient itself.
                            -- 'character' targets a specific EQ toon; 'name' is for
                            -- standalone actor clients (external apps) - wrong field.
                            mailbox:send(
                                { character=src.peer, script='ps_tracker', mailbox='ps_tracker' },
                                { type='do_trade', dest=entry.name, item=src.item.name, server=myServer }
                            )
                            printf("\ay[PS]\ax Sent trade request to \at%s\ax: give %s to %s",
                                src.peer, src.item.name, entry.name)
                        end
                    end
                    -- Hover: who gives what to whom, and what happens after the trade
                    if imgui.IsItemHovered() then
                        local shortName = src.item.name:gsub('^Sealed ', ''):gsub(' Source$', '')
                        local after = src.item.sealed and " (then convert and equip)" or " (then equip)"
                        imgui.BeginTooltip()
                        if src.peer == myName then
                            imgui.Text(("%s gives %s T%d to %s%s"):format(myName, shortName, src.item.tier, entry.name, after))
                            imgui.Text(("%s will walk to %s and open the trade."):format(myName, entry.name))
                        else
                            imgui.Text(("%s gives %s T%d to %s%s"):format(src.peer, shortName, src.item.tier, myName, after))
                            imgui.Text(("%s will walk to %s and open the trade."):format(src.peer, myName))
                        end
                        imgui.EndTooltip()
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
    imgui.TextDisabled(("Scan: %ds ago  |  Toons: %d"):format(scanAge, nPeers))

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
        printf("\ay[PS]\ax UI %s.", showUI and 'shown' or 'hidden')
    elseif arg == 'swap' then
        -- The work (which uses mq.delay) is done by the main loop, never inside this bind.
        -- A manual request lifts the automatic-retry pause and the convert retry gap.
        autoSwapFails        = 0
        lastAutoSwapAt       = 0
        lastConvertFailAt    = 0
        autoEquipPausedUntil = 0
        pendingManualSwap    = true
        printf("\ay[PS]\ax Swap requested - running on the next tick.")
    elseif arg == 'scan' then
        if swapping or equipSM.phase ~= 0 then
            printf("\ay[PS]\ax Scan skipped: a PS operation is running.")
        else
            equippedCache = getEquippedPS()
            bagsCache     = getPSInBags()
            peers[myName] = {
                name     = myName,
                class    = myClass,
                role     = myRole,
                equipped = equippedCache,
                bags     = sanitizeBags(bagsCache),
                ts       = mq.gettime(),
            }
            broadcastStatus()
            local nSealed = 0
            for _, v in ipairs(bagsCache) do
                if v.sealed then nSealed = nSealed + 1 end
            end
            printf("\ay[PS]\ax Equipped: %s | Bag PS: %d (sealed: %d)",
                   equippedCache and equippedCache.name or "None", #bagsCache, nSealed)
        end
    elseif arg == 'convert' then
        -- Deferred to the main loop like swap (it refreshes the bag cache and opens ItemDisplayWindow).
        pendingManualConvert = true
        printf("\ay[PS]\ax Convert requested - running on the next tick.")
    else
        printf("\ay[PS]\ax /ps stop | ui | swap | scan | convert")
    end
end)

-- ============================================================
-- Trade state machine - called each main loop tick (non-blocking)
-- ============================================================

-- Open state and partner name of the TradeWnd (see tradePartnerName).
local function tradeWndInfo()
    local tw = mq.TLO.Window('TradeWnd')
    if not (tw and tw.Open()) then return false, '', nil end
    return true, tradePartnerName(tw), tw
end

-- The TradeWnd partner label fills in a moment after the window opens. True while a blank name
-- should still be re-polled (up to BLANK_NAME_MS); a non-blank name resets the timer.
local BLANK_NAME_MS = 2000
local function blankNameWaiting(name, now)
    if name ~= '' then
        tradeSM.blankSince = 0
        return false
    end
    if tradeSM.blankSince == 0 then tradeSM.blankSince = now end
    return now - tradeSM.blankSince < BLANK_NAME_MS
end

-- Stop only the movement THIS run started (never the user's own /nav or /moveto).
local function stopOwnMove()
    if tradeSM.navStarted then
        local nav = mq.TLO.Plugin('MQ2Nav')
        if nav and nav.IsLoaded() == true and mq.TLO.Navigation.Active() then mq.cmd("/nav stop") end
        tradeSM.navStarted = false
    end
    if tradeSM.movetoStarted then
        mq.cmd("/moveto off")
        tradeSM.movetoStarted = false
    end
end

-- Abort the trade run and leave nothing behind: put our item back (only if the cursor
-- holds exactly the item we picked up), cancel a TradeWnd this run opened (and only while it
-- is still with the destination), stop our own movement, and tell the recipient the
-- announced trade is off.
-- The item on the cursor is "ours" only when THIS run issued the pickup and the cursor holds
-- exactly the item name (the cursor was verified empty before the pickup was issued).
local function ownItemOnCursor()
    local held = cursorName()
    return held ~= nil and tradeSM.pickIssued and held == tradeSM.itemName
end

-- Leave the trade run for good: nothing is held, no window is ours any more.
local function finishTradeRun()
    tradeSM.active     = false
    tradeSM.phase      = 'idle'
    tradeSM.ownsWnd    = false
    tradeSM.placed     = false
    tradeSM.pickIssued = false
    tradeSM.pickedUp   = false
    tradeSM.stackStage  = 0
    tradeSM.stackIssued = false
end

-- Close QuantityWnd (cancels the split: the stack stays in its bag slot). Window.DoClose is
-- in the definitions (_window.lua); that closing QuantityWnd cancels the split is UNVERIFIED
-- (no QTYW cancel-button name is documented or used by any script on this machine).
local function closeQuantityWnd()
    local qw = mq.TLO.Window('QuantityWnd')
    if qw and qw.Open() == true then qw.DoClose() end
end

-- Abort the run. This does not finish at once: it enters the bounded 'cancel' phase, in which
-- tradeStep keeps watching (up to TRADE_CANCEL_MS) for the item coming back from the closed
-- window onto the cursor and puts it away (see the 'cancel' branch). The run stays active (so
-- nothing else starts) until that phase ends.
local function abortTrade(reason)
    if tradeSM.phase == 'cancel' then return end
    printf('\ar[PS]\ax [trade] %s - aborting trade with %s.', reason, tostring(tradeSM.destName))
    stopOwnMove()
    local now = mq.gettime()
    -- A stack split in progress: close the quantity window first (nothing was placed anywhere).
    if tradeSM.stackIssued then closeQuantityWnd() end
    tradeSM.stackStage = 0
    if ownItemOnCursor() then
        safeAutoInventory('trade abort', tradeSM.itemName)
    elseif cursorName() then
        printf('\ar[PS]\ax [trade abort] "%s" on the cursor is not an item this run picked up - leaving it alone.', cursorName())
    end
    local closed = false
    -- A blank partner name (label not filled in yet) still counts as ours when this run owns the
    -- window and has placed the item in it.
    tradeSM.ownedItemWnd = tradeSM.ownsWnd and tradeSM.placed
    if tradeSM.ownsWnd then
        local open, his = tradeWndInfo()
        if open then
            if his == tradeSM.destName or (his == '' and tradeSM.ownedItemWnd) then
                cancelTradeWnd()   -- cancels the trade, items return to us
                tradeSM.closeTries = tradeSM.closeTries + 1
                closed = true
            else
                printf('\ay[PS]\ax [trade abort] the trade window is with "%s", not the destination - leaving it open.', his)
            end
        end
    end
    if tradeSM.announced then
        cancelTradeAnnouncement(tradeSM.destName, tradeSM.runId)
        tradeSM.announced = false
    end
    tradeSM.phase       = 'cancel'
    tradeSM.cancelUntil = now + TRADE_CANCEL_MS
    tradeSM.cancelMinAt = now + (closed and 1200 or 300)   -- time for returned items to show up
    tradeSM.cancelStows = 0
    tradeSM.nextTick    = now + 200
    tradeSM.placed      = false
end

-- Does the recipient still need this item according to its latest valid status?
-- (Checked right before the item is picked up / placed in the trade window.)
local function recipientStillNeeds()
    local p = peers[tradeSM.destName]
    if not isKnownPeer(tradeSM.destName) or not p then return false end
    return peerNeedsPS(p) and roleMatches(psRole(tradeSM.itemName), p.role or 'melee')
end

-- Pick exactly ONE item out of a stack onto the cursor. Runs one bounded, non-blocking step per
-- tradeStep tick (no mq.delay). `held` is the current cursor item name (or nil).
--   1. '/ctrl /itemnotify in packN M leftmouseup' (issued by the caller) is the split click on
--      this client (confirmed in game: one item lands on the cursor; Shift-click picked up the
--      whole stack). It either puts one item on the cursor directly (accepted below only when
--      Stack() == 1) or opens QuantityWnd, which steps 2 and 3 then drive. Only those
--      QuantityWnd steps are still UNTESTED in game (they are used only if a quantity window opens).
--   2. '/notify QuantityWnd QTYW_Slider newvalue 1': documented (notify.md, redchanges.txt) and used
--      by TCX/TCN/TSC scripts. The result is checked by reading Child('QTYW_SliderInput').Text()
--      (read in TSC grabber.lua/shared.lua; SetText on it is used in SoR-Missions echo.lua and is
--      the fallback on the 2nd retry). Only when it reads "1" is Accept clicked.
--   3. '/nomodkey /notify QuantityWnd QTYW_Accept_Button leftmouseup': used by all the scripts above.
--   Not verified anywhere on this machine: the QTYW_Cancel_Button name (so it is NOT used; the
--   window is closed with Window.DoClose instead, see closeQuantityWnd) and that no wininspect
--   dump of QuantityWnd exists.
-- Nothing is ever placed in the trade window from here. The caller only goes on to the click
-- when the cursor holds the expected name and Cursor.Stack() == 1 (verified again there).
-- Every failure goes through abortTrade, which closes QuantityWnd and stows the cursor item
-- (only the item this run picked up) with safeAutoInventory.
local function stackPickStep(now, held)
    local qw    = mq.TLO.Window('QuantityWnd')
    local qOpen = qw ~= nil and qw.Open() == true
    local stage = tradeSM.stackStage
    local name  = tradeSM.itemName

    local function pickedOk()
        printf('\ag[PS]\ax stack pick: picked 1 of %d "%s".', tradeSM.stackN, name)
        tradeSM.stackStage = 0
        tradeSM.nextTick   = now + 200
    end

    if stage == 1 then
        if held then
            -- The cursor filled without a quantity window: acceptable only when it is exactly
            -- one of the expected item (the stack had shrunk to 1 meanwhile).
            if held == name and not qOpen and (mq.TLO.Cursor.Stack() or 0) == 1 then
                pickedOk()
            else
                abortTrade(('stack pick aborted: the cursor filled without the quantity window ("%s" x%s)')
                    :format(held, tostring(mq.TLO.Cursor.Stack())))
            end
        elseif qOpen then
            mq.cmd('/notify QuantityWnd QTYW_Slider newvalue 1')
            tradeSM.stackStage = 2
            tradeSM.stackTries = 1
            tradeSM.stackUntil = now + STACK_WND_MS
            tradeSM.nextTick   = now + 250
        elseif now >= tradeSM.stackUntil then
            abortTrade('stack pick aborted: the quantity window did not open')
        else
            tradeSM.nextTick = now + 150
        end

    elseif stage == 2 then
        if held then
            abortTrade(('stack pick aborted: "%s" reached the cursor before Accept'):format(held))
        elseif not qOpen then
            abortTrade('stack pick aborted: the quantity window closed before Accept')
        else
            local inp = qw.Child('QTYW_SliderInput')
            local txt = ctlText(qw, 'QTYW_SliderInput')
            if txt == '1' then
                mq.cmd('/nomodkey /notify QuantityWnd QTYW_Accept_Button leftmouseup')
                tradeSM.stackStage = 3
                tradeSM.stackUntil = now + STACK_PICK_MS
                tradeSM.nextTick   = now + 300
            elseif now >= tradeSM.stackUntil then
                if tradeSM.stackTries >= 3 then
                    abortTrade(('stack pick aborted: the quantity input reads "%s", not 1'):format(tostring(txt)))
                else
                    tradeSM.stackTries = tradeSM.stackTries + 1
                    if tradeSM.stackTries == 2 and inp and inp() then
                        inp.SetText('1')
                    else
                        mq.cmd('/notify QuantityWnd QTYW_Slider newvalue 1')
                    end
                    tradeSM.stackUntil = now + STACK_WND_MS
                    tradeSM.nextTick   = now + 250
                end
            else
                tradeSM.nextTick = now + 150
            end
        end

    elseif stage == 3 then
        if not held then
            if qOpen then
                abortTrade('stack pick aborted: the quantity window is still open after Accept')
            elseif now >= tradeSM.stackUntil then
                abortTrade('stack pick aborted: nothing reached the cursor after Accept')
            else
                tradeSM.nextTick = now + 150
            end
        elseif held ~= name then
            abortTrade(('stack pick aborted: the cursor holds "%s", not "%s"'):format(held, name))
        elseif qOpen then
            abortTrade('stack pick aborted: the quantity window is still open after Accept')
        elseif (mq.TLO.Cursor.Stack() or 0) ~= 1 then
            abortTrade(('stack pick aborted: the cursor holds %s of "%s", not exactly 1')
                :format(tostring(mq.TLO.Cursor.Stack()), name))
        else
            pickedOk()
        end
    end
end

local function tradeStep()
    if not tradeSM.active then return end
    local now = mq.gettime()
    if now < tradeSM.nextTick then return end

    if tradeSM.phase == 'nav' then
        -- Abort when navigation takes too long or needs too many attempts.
        if now - tradeSM.navStartAt > NAV_TIMEOUT_MS or tradeSM.navTries >= NAV_MAX_TRIES then
            abortTrade(("could not reach %s (tries=%d, %ds)"):format(
                tradeSM.destName, tradeSM.navTries, math.floor((now - tradeSM.navStartAt) / 1000)))
            return
        end
        if not findInBags(tradeSM.itemName, true) then
            abortTrade("item is no longer in the bags (or is NO TRADE)")
            return
        end
        if not isKnownPeer(tradeSM.destName) or not validName(tradeSM.destName) then
            abortTrade("destination is no longer reporting")
            return
        end
        if (mq.TLO.Target.Name() or '') ~= tradeSM.destName or (mq.TLO.Target.Type() or '') ~= 'PC' then
            printf("\ay[PS]\ax [trade:nav] targeting %s", tradeSM.destName)
            -- Exact-name spawn search ("pc =Name"), so a similarly named mob/corpse is never picked
            mq.cmdf("/target pc =%s", tradeSM.destName)
            tradeSM.navTries = tradeSM.navTries + 1
            tradeSM.nextTick = now + 600
            return
        end
        local dist = mq.TLO.Target.Distance() or 999
        if dist > 12 then
            local nav = mq.TLO.Plugin('MQ2Nav')
            local mu  = mq.TLO.Plugin('MQ2MoveUtils')
            if nav and nav.IsLoaded() == true then
                if not mq.TLO.Navigation.Active() then
                    printf("\ay[PS]\ax [trade:nav] /nav spawn (dist=%.1f)", dist)
                    -- exact-name spawn search ("pc =Name"), validated letters-only above
                    mq.cmdf("/nav spawn pc =%s | distance=10", tradeSM.destName)
                    tradeSM.navStarted = true
                    tradeSM.navTries = tradeSM.navTries + 1
                end
            elseif mu and mu.IsLoaded() == true then
                -- MQ2Nav not loaded - fall back to /moveto (needs the target's spawn id).
                -- Issued once; re-issued only after a gap and only when it is no longer moving.
                -- "/moveto off" is sent on arrival, abort and script stop (stopOwnMove).
                local moving = mq.TLO.MoveTo.Moving() == true
                if not tradeSM.movetoStarted
                   or (now - tradeSM.movetoAt >= MOVETO_GAP_MS and not moving) then
                    local tid = mq.TLO.Target.ID()
                    if tid and tid > 0 then
                        printf("\ay[PS]\ax [trade:nav] MQ2Nav absent, /moveto id %d (dist=%.1f)", tid, dist)
                        mq.cmdf("/moveto id %d", tid)
                        tradeSM.movetoStarted = true
                        tradeSM.movetoAt      = now
                        tradeSM.navTries      = tradeSM.navTries + 1
                    end
                end
            else
                abortTrade("neither MQ2Nav nor MQ2MoveUtils is loaded")
                return
            end
            tradeSM.nextTick = now + 1000
        else
            stopOwnMove()
            printf("\ay[PS]\ax [trade:nav] reached %s (dist=%.1f) -> open phase", tradeSM.destName, dist)
            tradeSM.openTries = 0
            tradeSM.pickTries = 0
            tradeSM.openWaits = 0
            -- Tell the recipient (once) that this trade is coming, and wait (bounded) for its
            -- expect_ack before any window is opened: a recipient that has not registered the
            -- announcement would refuse the window and the run would only time out.
            if not tradeSM.announced then
                announceTrade(tradeSM.destName, tradeSM.itemName, tradeSM.runId)
                tradeSM.announced = true
            end
            tradeSM.acked    = false
            tradeSM.ackUntil = now + TRADE_ACK_MS
            tradeSM.phase    = 'ack'
            tradeSM.nextTick = now + 200
        end

    elseif tradeSM.phase == 'ack' then
        if tradeSM.acked then
            printf("\ay[PS]\ax [trade:ack] %s registered the announcement -> open phase", tradeSM.destName)
            tradeSM.phase    = 'open'
            tradeSM.nextTick = now + 200
        elseif not isKnownPeer(tradeSM.destName) then
            abortTrade("destination is no longer reporting")
        elseif now >= tradeSM.ackUntil then
            -- No window has been opened and nothing was picked up: the abort just withdraws the
            -- announcement (old peers never ack, so this is the expected outcome for them).
            abortTrade(("%s did not acknowledge the announcement in time"):format(tradeSM.destName))
        else
            tradeSM.nextTick = now + 200
        end

    elseif tradeSM.phase == 'open' then
        -- In EQ, /click left target on a PC only opens the trade window when you
        -- have an item on the cursor.  The mechanic is:
        --   1. Pick up the item via /ctrl /itemnotify -> item lands on cursor
        --   2. /click left target -> EQ opens TradeWnd and places cursor item in slot 0
        -- (Confirmed in EZInventory inventory_actor.lua lines 1427-1439.)
        -- The cursor must hold exactly the item we are giving; nothing else is ever traded.
        -- After the click the cursor is empty although the window may not be up yet, so a
        -- per-run flag (tradeSM.placed / clickedAt) stops a second copy being picked up.
        if equipSM.phase ~= 0 or swapping then
            -- the equip machine owns the cursor; wait (bounded)
            tradeSM.openWaits = tradeSM.openWaits + 1
            if tradeSM.openWaits > 60 then
                abortTrade("the equip machine kept the cursor too long")
                return
            end
            tradeSM.nextTick = now + 500
            return
        end
        local tradeOpen, hisName = tradeWndInfo()
        local held = cursorName()
        if tradeSM.stackStage == 0 then   -- the stack pick logs its own result (no per-tick spam)
            printf("\ay[PS]\ax [trade:open] try=%d TradeWnd=%s cursor=%s placed=%s",
                tradeSM.openTries, tostring(tradeOpen), tostring(held or 'empty'), tostring(tradeSM.placed))
        end

        if tradeOpen then
            -- A trade window is up: continue only when it is with our destination ...
            if blankNameWaiting(hisName, now) then
                tradeSM.nextTick = now + 200   -- partner label still filling in: re-poll (bounded)
                return
            end
            if hisName ~= tradeSM.destName then
                abortTrade(('trade window is with "%s", not the destination'):format(hisName))
                return
            end
            -- ... and only when THIS run put the item there. A window that was already open
            -- (or opened by someone else) is never adopted and Trade is never clicked on it.
            -- (The contents of the trade slots cannot be read: no definition exposes them.)
            if not tradeSM.placed then
                abortTrade("a trade window with the destination was already open - leaving it alone")
                return
            end
            printf("\ay[PS]\ax [trade:open] TradeWnd open -> confirm phase (item placed via cursor-click)")
            tradeSM.ownsWnd  = true
            tradeSM.phase    = 'confirm'
            tradeSM.nextTick = now + 300
            return
        end

        if tradeSM.placed then
            if now - tradeSM.clickedAt < PLACE_WAIT_MS then
                tradeSM.nextTick = now + 300   -- give the window time to open; do not pick again
                return
            end
            -- The window never opened, so our item is not in a trade: allow a bounded retry
            printf("\ay[PS]\ax [trade:open] no trade window after the click - retrying.")
            tradeSM.placed  = false
            tradeSM.ownsWnd = false
        end

        if tradeSM.openTries >= 5 then
            abortTrade(("could not open trade after %d tries"):format(tradeSM.openTries))
            return
        end

        -- The destination must still be targeted and close by
        if (mq.TLO.Target.Name() or '') ~= tradeSM.destName
           or (mq.TLO.Target.Distance() or 999) > 20 then
            abortTrade("target changed or destination moved away")
            return
        end

        -- A merchant/bank/loot window may have opened meanwhile: never pick up or click then.
        if uiBusy(true) then
            abortTrade("a merchant/bank/loot window opened")
            return
        end

        -- A stack split started by this run is in progress: it owns the cursor until it ends.
        if tradeSM.stackStage ~= 0 then
            if not recipientStillNeeds() then
                abortTrade("the recipient no longer needs this Power Source")
                return
            end
            stackPickStep(now, held)
            return
        end

        if not held then
            -- Step 1: find the item and pick it up onto the cursor (cursor is empty here).
            if tradeSM.pickTries >= 3 then
                abortTrade("could not pick the item up")
                return
            end
            local bag, slot = findInBags(tradeSM.itemName, true)
            if not bag then
                abortTrade("item not found in bags (or it is NO TRADE)")
                return
            end
            if not recipientStillNeeds() then
                abortTrade("the recipient no longer needs this Power Source")
                return
            end
            local srcItem = bagItem(bag, slot)
            local stackN  = srcItem and (srcItem.Stack() or 1) or 1
            if stackN > 1 then
                -- Stack: split exactly one off through QuantityWnd (see stackPickStep).
                local qw = mq.TLO.Window('QuantityWnd')
                if not STACK_TRADE_ENABLED then
                    abortTrade("the item is a stack and stack trading is disabled")
                    return
                end
                if qw and qw.Open() == true then
                    abortTrade("a quantity window is already open - not touching it")
                    return
                end
                tradeSM.pickTries   = tradeSM.pickTries + 1
                tradeSM.pickIssued  = true   -- a cursor item with this name now came from THIS run
                tradeSM.stackIssued = true   -- QuantityWnd must be closed on every exit from here
                tradeSM.stackStage  = 1
                tradeSM.stackN      = stackN
                tradeSM.stackTries  = 0
                tradeSM.stackUntil  = now + STACK_WND_MS
                printf('\ay[PS]\ax stack pick: taking 1 of %d "%s" (pack%d slot %d)',
                    stackN, tradeSM.itemName, bag, slot)
                -- Ctrl-click is the split/single pickup on this client (confirmed by the user; a
                -- Shift-click picked up the WHOLE stack and was caught by the Stack()==1 check).
                -- Either one item lands on the cursor, or QuantityWnd opens (handled by stackPickStep).
                if slot == 0 then
                    mq.cmdf("/ctrl /itemnotify pack%d leftmouseup", bag)
                else
                    mq.cmdf("/ctrl /itemnotify in pack%d %d leftmouseup", bag, slot)
                end
                tradeSM.nextTick = now + 400   -- wait for QuantityWnd to open
                return
            end
            tradeSM.pickTries = tradeSM.pickTries + 1
            tradeSM.pickIssued = true   -- from here on a cursor item with this name came from THIS run
            if slot == 0 then
                printf('\ay[PS]\ax [trade:open] picking up "%s" from pack%d (top-level)',
                    tradeSM.itemName, bag)
                mq.cmdf("/ctrl /itemnotify pack%d leftmouseup", bag)
            else
                printf('\ay[PS]\ax [trade:open] picking up "%s" from pack%d slot %d',
                    tradeSM.itemName, bag, slot)
                mq.cmdf("/ctrl /itemnotify in pack%d %d leftmouseup", bag, slot)
            end
            tradeSM.nextTick = now + 500   -- wait for pick-up animation
        elseif held == tradeSM.itemName and not tradeSM.pickIssued then
            -- The cursor already held this item before this run picked anything up: it is not ours
            -- to place or to stow. Abort without touching it.
            abortTrade(('cursor already holds "%s" and this run did not pick it up'):format(held))
        elseif held == tradeSM.itemName then
            -- Step 2: cursor holds exactly the item (picked up by THIS run) - click target to open
            -- the trade window. This places the cursor item into trade slot 0 and opens TradeWnd.
            tradeSM.pickedUp = true   -- verified pickup
            -- Re-read the cursor item: a spent, NO TRADE PS, or a cursor holding more than one
            -- item, must never be handed over (Cursor.Stack is exactly 1, or the run aborts and the
            -- item is stowed).
            local cur = mq.TLO.Cursor
            if not psSealed(held) and isItemSpent(cur) then
                abortTrade(('"%s" on the cursor is spent'):format(held))
                return
            end
            if not tradeable(cur) then
                abortTrade(('"%s" on the cursor is NO TRADE'):format(held))
                return
            end
            if (cur.Stack() or 1) ~= 1 then
                abortTrade(('the cursor holds %s of "%s", not exactly 1'):format(tostring(cur.Stack()), held))
                return
            end
            -- Last look at the recipient's latest status before the item goes into the window:
            -- abortTrade puts the item back (cursor verified) and withdraws the announcement.
            if not recipientStillNeeds() then
                abortTrade("the recipient no longer needs this Power Source")
                return
            end
            printf('\ay[PS]\ax [trade:open] cursor has "%s", left-clicking target (attempt %d)',
                held, tradeSM.openTries + 1)
            mq.cmd("/click left target")
            -- The click may open TradeWnd with our item in it (even after the last try's
            -- wait): from here on this run owns the window, so every exit path cancels it.
            tradeSM.placed    = true
            tradeSM.clickedAt = now
            tradeSM.ownsWnd   = true
            tradeSM.openTries = tradeSM.openTries + 1
            tradeSM.nextTick  = now + 800
        else
            -- Something else is on the cursor: never trade it, never touch it.
            abortTrade(('cursor holds "%s", not the item to give'):format(held))
        end

    elseif tradeSM.phase == 'confirm' then
        local open, hisName, twConfirm = tradeWndInfo()
        if not open or not twConfirm then
            abortTrade("TradeWnd closed before confirm")
            return
        end
        if blankNameWaiting(hisName, now) then
            tradeSM.nextTick = now + 200   -- partner label still filling in: re-poll (bounded)
            return
        end
        if hisName ~= tradeSM.destName then
            abortTrade(('trade partner is "%s", expected the destination'):format(hisName))
            return
        end
        if not tradeSM.placed then
            abortTrade("this run did not place an item in the trade window")
            return
        end
        if cursorName() then
            abortTrade("the item was not placed in the trade window")
            return
        end
        local tradeBtn = twConfirm.Child('TRDW_Trade_Button')
        if not tradeBtn or not tradeBtn() then
            abortTrade("TRDW_Trade_Button child not found")
            return
        end
        printf("\ay[PS]\ax [trade:confirm] clicking Trade button")
        tradeBtn.LeftMouseUp()
        printf("\ag[PS]\ax Trade sent to %s - waiting for accept.", tradeSM.destName)
        tradeSM.phase     = 'wait'
        tradeSM.waitUntil = now + TRADE_WAIT_MS
        tradeSM.nextTick  = now + 500

    elseif tradeSM.phase == 'wait' then
        -- Wait for the window to close (accepted); cancel it if the recipient never accepts.
        -- The window must still be the trade with our destination: never wait on, or close,
        -- somebody else's trade.
        local open, hisName = tradeWndInfo()
        if not open then
            -- A closed window is not proof of a completed trade (it may have been cancelled):
            -- verify in the 'verify' phase before the announcement is left standing.
            printf("\ay[PS]\ax [trade:wait] trade window closed - verifying the trade completed.")
            tradeSM.ownsWnd   = false
            tradeSM.phase     = 'verify'
            tradeSM.waitUntil = now + VERIFY_WAIT_MS
            tradeSM.nextTick  = now + 300
        elseif hisName == '' then
            -- Blank partner label: never end the run with our item still in an open window. Keep
            -- waiting; the waitUntil bound below cancels the window if it never resolves.
            if now >= tradeSM.waitUntil then
                abortTrade("recipient did not accept in time (partner name unreadable)")
            else
                tradeSM.nextTick = now + 500
            end
        elseif hisName ~= tradeSM.destName then
            printf('\ay[PS]\ax [trade:wait] the trade window is now with "%s" - ending this run without touching it.', hisName)
            if tradeSM.announced then
                cancelTradeAnnouncement(tradeSM.destName, tradeSM.runId)
                tradeSM.announced = false
            end
            finishTradeRun()
        elseif now >= tradeSM.waitUntil then
            abortTrade("recipient did not accept in time")
        else
            tradeSM.nextTick = now + 500
        end

    elseif tradeSM.phase == 'verify' then
        -- Positive evidence is required, over the WHOLE VERIFY_WAIT_MS window: TradeWnd stays
        -- closed, the cursor stays empty, and the bags keep fewer copies than when the run started.
        -- A cancelled trade returns the item, possibly late (cursor momentarily empty, bag count
        -- still lower): so success is only declared when the window has run out with all of this
        -- true on every tick. If the item reappears on the cursor or in the bags, or the window
        -- reopens, at any point, the run is NOT complete: put our item away (only one this run
        -- picked up) and withdraw the announcement (scoped to this run id).
        local held = cursorName()
        local open = tradeWndInfo()
        local done, completed = false, false
        if held and held == tradeSM.itemName then
            if ownItemOnCursor() then safeAutoInventory('trade verify', tradeSM.itemName) end
            printf("\ay[PS]\ax [trade:verify] the item came back - trade was not completed.")
            done = true
        elseif open then
            printf("\ay[PS]\ax [trade:verify] a trade window opened again - trade was not completed (window left alone).")
            done = true
        elseif countInBags(tradeSM.itemName) >= tradeSM.countBefore then
            printf("\ay[PS]\ax [trade:verify] the item is back in the bags - trade was not completed.")
            done = true
        elseif now >= tradeSM.waitUntil then
            if held == nil then
                printf("\ag[PS]\ax [trade:verify] the item stayed gone for the whole window - trade finished.")
                completed = true
            else
                printf("\ay[PS]\ax [trade:verify] could not confirm the trade completed - treating it as not completed.")
            end
            done = true
        end
        if done then
            if completed then
                tradeSM.announced = false   -- consumed by the recipient; nothing to withdraw
            elseif tradeSM.announced then
                cancelTradeAnnouncement(tradeSM.destName, tradeSM.runId)
                tradeSM.announced = false
            end
            finishTradeRun()
        else
            tradeSM.nextTick = now + 300
        end

    elseif tradeSM.phase == 'cancel' then
        -- Bounded return-and-stow phase after an abort (also used at shutdown): the window was
        -- closed (only if it is ours and still with the destination); items returned by closing it
        -- may land on the cursor a moment later. Poll up to TRADE_CANCEL_MS; the cursor item is
        -- put away only when this run picked it up (ownItemOnCursor) and the name matches.
        local open, hisName, tw = tradeWndInfo()
        local ourWndOpen = open and tradeSM.ownsWnd
                           and (hisName == tradeSM.destName or (hisName == '' and tradeSM.ownedItemWnd))
        if ourWndOpen and tw and tradeSM.closeTries < 12 then
            cancelTradeWnd()   -- click Cancel first, DoClose on a repeat tick
            tradeSM.closeTries = tradeSM.closeTries + 1
        end
        -- A quantity window this run opened (possibly late) is closed; it is never left behind.
        local qtyOpen = false
        if tradeSM.stackIssued then
            local qw = mq.TLO.Window('QuantityWnd')
            qtyOpen = qw ~= nil and qw.Open() == true
            if qtyOpen then closeQuantityWnd() end
        end
        if ownItemOnCursor() and tradeSM.cancelStows < 3 and not uiBusy(true) then
            tradeSM.cancelStows = tradeSM.cancelStows + 1
            safeAutoInventory('trade cancel', tradeSM.itemName)
            tradeSM.cancelMinAt = math.max(tradeSM.cancelMinAt, now + 400)
        end
        if now >= tradeSM.cancelUntil
           or (not ourWndOpen and not qtyOpen and not ownItemOnCursor() and now >= tradeSM.cancelMinAt) then
            if qtyOpen then
                printf('\ar[PS]\ax [trade cancel] the quantity window is still open - close it by hand.')
            end
            if ownItemOnCursor() then
                printf('\ar[PS]\ax [trade cancel] "%s" is still on the cursor - leaving it there.', tradeSM.itemName)
            end
            finishTradeRun()
        else
            tradeSM.nextTick = now + 200
        end
    end
end

mq.imgui.init('PSTracker', renderUI)

-- ============================================================
-- Deferred requests (run by the main loop, where mq.delay is allowed)
-- ============================================================

-- Refresh the own caches and the own entry in the peer table.
local function refreshSelf(now)
    equippedCache = getEquippedPS()
    bagsCache     = getPSInBags()
    peers[myName] = {
        name     = myName,
        class    = myClass,
        role     = myRole,
        equipped = equippedCache,
        bags     = sanitizeBags(bagsCache),
        ts       = now,
    }
end

-- First fresh sealed PS in the own bags that fits the role (bagsCache is sorted best-first).
local function firstOwnSealed()
    for _, item in ipairs(bagsCache) do
        if not item.spent and item.sealed and roleMatches(item.role, myRole) then return item end
    end
    return nil
end

-- Swap button on this toon / do_swap from a peer. Only ever uses the toon's OWN bags: if
-- nothing usable is there, the user is told to click Trade; no trade is started or requested.
local function runForcedSwap(now)
    refreshSelf(now)
    -- Only act when this toon really needs a different PS (a swap request from
    -- elsewhere must never pull a good PS out of the slot).
    local needsSwap = not equippedCache or equippedCache.spent
                      or not roleMatches(equippedCache.role, myRole)
    if not needsSwap then
        printf("\ay[PS]\ax Swap request ignored: \at%s\ax already has a suitable PS equipped.", myName)
    elseif opBusy() or uiBusy() then
        printf("\ay[PS]\ax Swap request ignored: busy (another PS operation or a trade/merchant/bank/loot window).")
    else
        printf("\ay[PS]\ax Swap: running forced equip for \at%s\ax (role=%s)", myName, myRole)
        local ok, why = trySwapPS(true, true)
        if ok then
            refreshSelf(now)
            printf("\ay[PS]\ax Swap: equip succeeded - cache refreshed.")
        elseif why == 'busy' or why == 'failed' then
            -- A step was skipped or went wrong: do not do anything else now
            printf("\ay[PS]\ax Swap: not completed (%s) - try again in a moment.", why)
        elseif why == 'fresh' then
            printf("\ay[PS]\ax Swap request ignored: \at%s\ax already has a suitable PS equipped.", myName)
        else
            -- No suitable non-sealed PS in own bags: convert an own sealed one, otherwise
            -- hint that a trade can be requested by hand (never started from here).
            local sealed = firstOwnSealed()
            if sealed then
                if startConvert(sealed) then
                    printf("\ay[PS]\ax Swap: converting sealed \ag%s\ax (T%d) in own bags",
                           sealed.name, sealed.tier)
                end
            else
                local src = findTradeSource(myRole, myName)
                if src then
                    hintPeerSource(src, true)
                else
                    printf("\ay[PS]\ax \arSwap: no %s PS found in any toon's bags.\ax", myRole)
                end
            end
        end
    end
end

-- /ps swap
local function runManualSwap(now)
    refreshSelf(now)
    if equippedCache and not equippedCache.spent then
        printf("\ay[PS]\ax %s is not spent - no swap needed.", equippedCache.name)
        return
    end
    -- Works for a spent PS and for an empty slot alike
    local ok, why = trySwapPS()
    if ok then
        refreshSelf(now)
    elseif why == 'sealed' then
        -- a role-matching sealed PS is in the bags: convert it, then equip it
        local sealed = firstOwnSealed()
        if sealed and startConvert(sealed) then
            printf("\ay[PS]\ax Swap: converting sealed \ag%s\ax (T%d) in own bags",
                   sealed.name, sealed.tier)
        end
    elseif why == 'busy' then
        printf("\ay[PS]\ax Swap not started: busy (another PS operation or a trade/merchant/bank/loot window).")
    elseif why == 'fresh' then
        printf("\ay[PS]\ax A suitable PS is already equipped - no swap needed.")
    elseif not why then
        local src = findTradeSource(myRole, myName)
        if src then hintPeerSource(src, true) end
    end
end

-- /ps convert
local function runManualConvert(now)
    refreshSelf(now)
    local sealed = nil
    for _, item in ipairs(bagsCache) do
        if item.sealed then sealed = item; break end
    end
    if not sealed then
        printf("\ay[PS]\ax No sealed PS found in bags to convert.")
        return
    end
    -- Inspect() + IDW_ConvertButton - same path as auto-equip.
    -- The old /itemnotify rightmouseup approach requires the bag window
    -- to be open and fails silently when it isn't.
    -- convertOnly: the unsealed item is left in the bags, not equipped.
    if startConvert(sealed, true) then
        printf("\ay[PS]\ax Converting \ag%s\ax (pack%d slot %d) - opening ItemDisplayWindow...",
               sealed.name, sealed.bag, sealed.slot)
    end
end

-- ============================================================
-- Main loop
-- ============================================================
printf("\ay[PS]\ax Running for \at%s\ax (%s / %s). /ps stop to quit.",
       myName, myClass, myRole)

while running do
    local now = mq.gettime()

    -- Requests set by binds, the UI and actor messages are acted on every tick
    -- (not only at the 5 s scan), so a Swap click is immediate.
    if pendingForceSwap then
        pendingForceSwap = false
        runForcedSwap(now)
        now = mq.gettime()
    end
    if pendingManualSwap then
        pendingManualSwap = false
        runManualSwap(now)
        now = mq.gettime()
    end
    if pendingManualConvert then
        pendingManualConvert = false
        runManualConvert(now)
        now = mq.gettime()
    end

    if now - lastScan >= SCAN_MS then
        refreshSelf(now)

        -- Forget peers that have been silent for a long time (they stay visible, marked
        -- stale, until then)
        for n, p in pairs(peers) do
            if n ~= myName and now - (p.ts or 0) > PEER_PRUNE_MS then peers[n] = nil end
        end

        if (not equippedCache or equippedCache.spent) and now >= autoEquipPausedUntil then
            -- Automatic attempts are bounded: a gap after any failure, and a pause after
            -- SWAP_MAX_FAILS failures (a manual /ps swap lifts the pause).
            if autoSwapFails < SWAP_MAX_FAILS
               and (lastAutoSwapAt == 0 or now - lastAutoSwapAt >= SWAP_RETRY_MS) then
                local swapped, why = trySwapPS()
                if swapped then
                    refreshSelf(now)
                    autoSwapFails = 0
                elseif why == 'busy' then
                    lastAutoSwapAt = now
                elseif why == 'fresh' then
                    refreshSelf(now)   -- the slot was already fine: the cache was stale
                elseif why == 'failed' then
                    lastAutoSwapAt = now
                    autoSwapFails  = autoSwapFails + 1
                    if autoSwapFails >= SWAP_MAX_FAILS then
                        printf("\ar[PS]\ax Auto-equip failed %d times - pausing automatic attempts. Use /ps swap to retry.",
                               autoSwapFails)
                    end
                elseif why == 'sealed' and equipSM.phase == 0 and not tradeSM.active then
                    -- trySwapPS stepped aside because of a sealed own item: convert it
                    -- (own bags are automatic; peers are never asked automatically).
                    -- bagsCache is sorted best-first, so the first match is the best one.
                    local sealed = firstOwnSealed()
                    if sealed and startConvert(sealed) then
                        printf("\ay[PS]\ax Auto-equip: converting sealed \ag%s\ax (T%d) in own bags",
                               sealed.name, sealed.tier)
                    else
                        lastAutoSwapAt = now   -- refused (cursor/busy/window): do not retry every scan
                    end
                elseif why == nil then
                    -- Nothing usable in own bags. If only a peer holds a PS, hint (throttled).
                    local src = findTradeSource(myRole, myName)
                    if src then hintPeerSource(src, false) end
                end
            end
        elseif equippedCache and not equippedCache.spent then
            -- Healthy again: clear the failure counter
            autoSwapFails = 0
        end

        lastScan = now
    end

    if now - lastBroadcast >= BCAST_MS then
        broadcastStatus()
        lastBroadcast = now
    end

    -- Restart All: start the peers again once they have said goodbye
    -- Each step is guarded: a Lua error is logged once (throttled) and the script keeps running.
    local okR, errR = pcall(restartStep, mq.gettime())
    if not okR then
        logThrottled('err_restart', 15000, '\ar[PS]\ax restartStep error: %s', tostring(errR))
    end

    local okT, errT = pcall(tradeStep)
    if not okT then
        logThrottled('err_trade', 15000, '\ar[PS]\ax tradeStep error: %s', tostring(errT))
        if tradeSM.active and tradeSM.phase == 'cancel' then
            -- the cancel phase itself raised: it is time-bounded, so end the run when its time is up
            if mq.gettime() >= tradeSM.cancelUntil then finishTradeRun() end
        elseif tradeSM.active then
            local okA, errA = pcall(abortTrade, 'internal error')
            if not okA then
                logThrottled('err_abort', 15000, '\ar[PS]\ax abortTrade error: %s', tostring(errA))
                -- the abort itself failed: release the run so nothing keeps retrying a broken state
                tradeSM.phase = 'cancel'
                tradeSM.cancelUntil = mq.gettime()
            end
        end
    end

    local okC, errC = pcall(autoAcceptStep)
    if not okC then
        logThrottled('err_accept', 15000, '\ar[PS]\ax autoAcceptStep error: %s', tostring(errC))
    end
    mq.doevents()
    mq.delay('100ms')
end

-- Shutdown: leave no half-finished trade, open window or held item behind
if tradeSM.active then
    abortTrade("script stopping")
    -- Run the bounded return-and-stow phase to its end (main thread: mq.delay is allowed here).
    -- Capped by wall time as well, so shutdown can never hang.
    local shutdownStart = mq.gettime()
    while tradeSM.active and mq.gettime() - shutdownStart < TRADE_CANCEL_MS + 1500 do
        pcall(tradeStep)
        mq.delay('100ms')
    end
    if tradeSM.active then finishTradeRun() end
end
-- An incoming trade we auto-accepted but that the giver never confirmed: cancel it, but only when
-- the open window is still with the same partner we accepted. Any other (ignored or unrelated)
-- window is left alone.
if tradeAccepted and tradeAcceptedPartner ~= '' then
    local open, his = tradeWndInfo()
    if open and his == tradeAcceptedPartner then
        printf('\ay[PS]\ax Shutdown: cancelling the accepted incoming trade with "%s".', his)
        -- main thread: bounded poll (click Cancel, then DoClose on the repeat call)
        local cancelStart = mq.gettime()
        while cancelTradeWnd() and mq.gettime() - cancelStart < 1500 do
            mq.delay('100ms')
        end
    end
end
if equipSM.phase >= 2 then
    safeAutoInventory('shutdown', equipSM.priorPS)
elseif equipSM.phase == 1 and equipSM.convertBtnClicked then
    -- Convert was clicked: the converted item may already be on the cursor. Put it away only
    -- when it is the expected result or a known Power Source (safeAutoInventory leaves anything
    -- else alone).
    safeAutoInventory('shutdown', equipSM.expectName)
end
resetEquipSM()
-- Tell the peers we are gone, so they drop us at once (and Restart All can start us again)
mailbox:send({ script='ps_tracker', mailbox='ps_tracker' }, { type='bye', name=myName, server=myServer })

printf("\ay[PS]\ax Stopped.")
